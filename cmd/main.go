package main

import (
	"context"
	"math/rand"
	"regexp"
	"time"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"

	"github.com/bigredeye/concurrency_watcher/internal/config"
	"github.com/bigredeye/concurrency_watcher/internal/gitlab"
	"github.com/bigredeye/concurrency_watcher/internal/logging"
	"github.com/bigredeye/concurrency_watcher/internal/sheets"
	"github.com/bigredeye/concurrency_watcher/internal/types"
)

func main() {
	if err := run(); err != nil {
		log.WithError(err).Fatalln("Process failed")
	}
}

type Daemon struct {
	gitlab *gitlab.Client
	sheets *sheets.Client
}

func newDaemon(conf *config.Config) (*Daemon, error) {
	gitlabClient, err := gitlab.NewClient("https://gitlab.com", conf.GitLabToken)
	if err != nil {
		log.WithError(err).Errorln("Failed to initialize gitlab client")
		return nil, err
	}

	googleClient, err := sheets.NewClient(context.Background(), conf.GoogleCredentialsPath)
	if err != nil {
		log.WithError(err).Errorln("Failed to initialize google client")
		return nil, err
	}

	return &Daemon{
		gitlab: gitlabClient,
		sheets: googleClient,
	}, nil
}

func run() error {
	rand.Seed(time.Now().Unix())

	if err := godotenv.Load(); err != nil {
		log.WithError(err).Warn("Failed to load .env file")
	}

	if err := logging.InitLogging(""); err != nil {
		return err
	}

	config, err := config.LoadConfig()
	if err != nil {
		return err
	}
	log.Infoln("Successfully loaded config")

	daemon, err := newDaemon(config)
	if err != nil {
		return err
	}

	runIter := func() error {
		group, err := daemon.gitlab.ListGroupRequests(config.GitLabGroup)
		if err != nil {
			log.WithError(err).Fatalln("Failed to list group merge requests")
		}
		log.Printf("Found %d merge requests", group.MergeRequests.Count)

		err = daemon.sheets.WithSnapshot(config.GoogleSpreadsheetId, "Merge Requests", func(snapshot *sheets.Snapshot) error {
			if err := snapshot.Delete().Do(); err != nil {
				log.WithError(err).Errorln("Failed to clear table")
				return err
			}

			query := snapshot.Insert().Into("Student", "Task", "Merge request title", "Created at", "Merge status", "Pipeline status", "Url")

			titleParser := newMergeRequestTitleParser()
			for _, mr := range group.MergeRequests.Nodes {
				info := titleParser.parse(mr)
				query.Values(info.student, info.task, mr.Title, mr.CreatedAt, mr.MergeStatus, mr.HeadPipeline.Status, mr.WebUrl)
			}
			if err := query.Do(); err != nil {
				log.WithError(err).Errorln("Failed to append merge requests to the table")
				return err
			}

			if err := snapshot.Sort().By("Username", "Title").Do(); err != nil {
				log.WithError(err).Errorln("Failed to sort table")
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}

		log.Infoln("Successfully updated table")
		return nil
	}

	for {
		if err := runIter(); err != nil {
			log.WithError(err).Warn("Iteration failed")
		}
		time.Sleep(time.Second * 60)
	}
}

type mergeRequestTitleParser struct {
	re *regexp.Regexp
}

type mergeRequestTitle struct {
	unversity string
	student   string
	task      string
}

func newMergeRequestTitleParser() *mergeRequestTitleParser {
	re := regexp.MustCompile(`^\[(\w+)\] \[(\w+)-(\w+)\] (.+/.+)$`)
	return &mergeRequestTitleParser{
		re: re,
	}
}

func (s *mergeRequestTitleParser) parse(mr *types.MergeRequest) *mergeRequestTitle {
	groups := s.re.FindStringSubmatch(mr.Title)
	if len(groups) != 5 {
		return &mergeRequestTitle{
			unversity: "unknown",
			student:   "@" + mr.Author.Username,
			task:      mr.Title,
		}
	}
	return &mergeRequestTitle{
		unversity: groups[1],
		student:   groups[2] + " " + groups[3],
		task:      groups[4],
	}
}
