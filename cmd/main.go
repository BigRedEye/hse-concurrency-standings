package main

import (
	"context"
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
		log.Println("Found %s merge requests", group.MergeRequests.Count)

		// FIXME(sskvor): Emulate transactions
		if err := daemon.sheets.Delete(config.GoogleSpreadsheetId, "Merge Requests").Do(); err != nil {
			log.WithError(err).Errorln("Failed to clear table")
			return err
		}

		nameParser := newStudentNameParser()

		query := daemon.sheets.Insert(config.GoogleSpreadsheetId, "Merge Requests").Into("Student", "Title", "Created at", "Merge status", "Pipeline status", "Url")
		for _, mr := range group.MergeRequests.Nodes {
			name := nameParser.parse(mr)
			query.Values(name, mr.Title, mr.CreatedAt, mr.MergeStatus, mr.HeadPipeline.Status, mr.WebUrl)
		}
		if err := query.Do(); err != nil {
			log.WithError(err).Errorln("Failed to append merge requests to the table")
			return err
		}

		if err := daemon.sheets.Sort(config.GoogleSpreadsheetId, "Merge Requests").By("Username", "Title").Do(); err != nil {
			log.WithError(err).Errorln("Failed to sort table")
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

type studentNameParser struct {
	re *regexp.Regexp
}

func newStudentNameParser() *studentNameParser {
	re := regexp.MustCompile(`^\[hse\] \[(\w+)-(\w+)\] .+/.+$`)
	return &studentNameParser{
		re: re,
	}
}

func (s *studentNameParser) parse(mr *types.MergeRequest) string {
	groups := s.re.FindStringSubmatch(mr.Title)
	if len(groups) < 3 {
		return "@" + mr.Author.Username
	}
	return groups[1] + " " + groups[2]
}
