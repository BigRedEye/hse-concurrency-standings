package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

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
	config *config.Config
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
		config: conf,
		gitlab: gitlabClient,
		sheets: googleClient,
	}, nil
}

type DeadlinesGroup struct {
	Group    string
	Start    string
	Deadline string
	Tasks    []struct {
		Task  string
		Score int
	}
}

func (d *Daemon) listTasksFromDeadlines() ([]string, error) {
	tasks := make([]string, 0)
	res, err := http.Get(d.config.DeadlinesUrl)

	if err != nil {
		return tasks, err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return tasks, err
	}

	deadlines := make([]DeadlinesGroup, 0)
	err = yaml.Unmarshal(body, &deadlines)
	if err != nil {
		log.WithError(err).Warnf("Failed to decode deadlines.yml")
		return tasks, err
	}

	for _, group := range deadlines {
		for _, task := range group.Tasks {
			tasks = append(tasks, task.Task)
		}
	}

	return tasks, nil
}

func run() error {
	rand.Seed(time.Now().Unix())

	if err := godotenv.Load(); err != nil {
		log.WithError(err).Warn("Failed to load .env file")
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if err := logging.InitLogging(logLevel); err != nil {
		return err
	}
	log.Infof("Initialized logging using %s level", logLevel)

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
		tasks, err := daemon.listTasksFromDeadlines()
		if err != nil {
			return fmt.Errorf("Failed to get tasks from deadlines.yml: %w", err)
		}

		taskToIndex := make(map[string]int)
		for i, task := range tasks {
			taskToIndex[task] = i
			log.Debugf("Task %s", task)
		}
		log.Infof("Found %d tasks", len(tasks))

		mergeRequestsByStudent := make(map[string][]*mergeRequestTitle)
		group, err := daemon.gitlab.ListGroupRequests(config.GitLabGroup)
		if err != nil {
			log.WithError(err).Errorln("Failed to list group merge requests")
			return err
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
				if _, found := mergeRequestsByStudent[info.student]; !found {
					mergeRequestsByStudent[info.student] = make([]*mergeRequestTitle, 0, 1)
				}
				mergeRequestsByStudent[info.student] = append(mergeRequestsByStudent[info.student], info)
				query.Values(info.student, info.task, mr.Title, mr.CreatedAt, mr.MergeStatus, mr.HeadPipeline.Status, mr.WebUrl)
			}
			if err := query.Do(); err != nil {
				log.WithError(err).Errorln("Failed to append merge requests to the table")
				return err
			}

			if err := snapshot.Sort().By("Student", "Task").Do(); err != nil {
				log.WithError(err).Errorln("Failed to sort table")
				return err
			}

			return nil
		})
		if err != nil {
			log.WithError(err).Warn("Failed to update Merge Requests table")
			return err
		}
		log.Infoln("Successfully updated Merge Requests table")

		err = daemon.sheets.WithSnapshot(config.GoogleSpreadsheetId, "Results", func(snapshot *sheets.Snapshot) error {
			if err := snapshot.Delete().Do(); err != nil {
				log.WithError(err).Errorln("Failed to clear table")
				return err
			}

			columns := append([]string{"Student"}, tasks...)
			query := snapshot.Insert().Into(columns...)

			students := make([]string, 0)
			for student := range mergeRequestsByStudent {
				students = append(students, student)
			}
			sort.Strings(students)
			for _, student := range students {
				// fmt.Println(k, mergeRequestsByStudent[k])
				values := make([]interface{}, len(tasks)+1)
				values[0] = student

				for _, mr := range mergeRequestsByStudent[student] {
					text, color := classifyMergeRequestStatus(mr)

					values[1+taskToIndex[mr.task]] = sheets.Cell{
						Text:            text,
						Hyperlink:       mr.url,
						BackgroundColor: color,
					}
				}

				query.Values(values...)
			}
			if err := query.Do(); err != nil {
				log.WithError(err).Errorln("Failed to append merge requests to the table")
				return err
			}

			if err := snapshot.Sort().By("Student", "Task").Do(); err != nil {
				log.WithError(err).Errorln("Failed to sort table")
				return err
			}

			return nil
		})
		if err != nil {
			log.WithError(err).Warn("Failed to update Merge Requests table")
			return err
		}
		log.Infoln("Successfully updated Results table")

		return nil
	}

	for {
		if err := runIter(); err != nil {
			log.WithError(err).Warn("Iteration failed")
		}
		time.Sleep(config.IterationInterval)
	}
}

type mergeRequestTitleParser struct {
	re *regexp.Regexp
}

type mergeRequestTitle struct {
	unversity string
	student   string
	task      string
	url       string

	pipelineStatus      string
	mergeStatus         string
	numProblems         int
	numResolvedProblems int
	approved            bool
}

func newMergeRequestTitleParser() *mergeRequestTitleParser {
	re := regexp.MustCompile(`^\[(\w+)\] \[(\w+)-(\w+)\] (.+/.+)$`)
	return &mergeRequestTitleParser{
		re: re,
	}
}

func (s *mergeRequestTitleParser) parse(mr *types.MergeRequest) *mergeRequestTitle {
	res := &mergeRequestTitle{
		url:                 mr.WebUrl,
		pipelineStatus:      mr.HeadPipeline.Status,
		mergeStatus:         mr.MergeStatus,
		numProblems:         0,
		numResolvedProblems: 0,
		approved:            len(mr.ApprovedBy.Nodes) > 0,
	}

	for _, discussion := range mr.Discussions.Nodes {
		if discussion.Resolvable {
			res.numProblems++
			if discussion.Resolved {
				res.numResolvedProblems++
			}
		}
	}

	groups := s.re.FindStringSubmatch(mr.Title)
	if len(groups) != 5 {
		res.unversity = "unknown"
		res.student = "@" + mr.Author.Username
		res.task = mr.Title
	} else {
		res.unversity = groups[1]
		res.student = groups[2] + " " + groups[3]
		res.task = groups[4]
	}
	return res
}

var (
	LightRed = &sheets.Color{
		Red:   0.95,
		Green: 0.80,
		Blue:  0.80,
	}
	LightGreen = &sheets.Color{
		Red:   0.85,
		Green: 0.91,
		Blue:  0.82,
	}
	LightYellow = &sheets.Color{
		Red:   1.00,
		Green: 0.94,
		Blue:  0.80,
	}
	LightPurple = &sheets.Color{
		Red:   0.85,
		Green: 0.82,
		Blue:  0.91,
	}
)

func classifyMergeRequestStatus(mr *mergeRequestTitle) (string, *sheets.Color) {
	if mr.approved {
		return "Approved", LightGreen
	}

	if mr.pipelineStatus != "SUCCESS" {
		return "Pipeline failed", LightRed
	}

	if mr.numProblems > mr.numResolvedProblems {
		return "Unresolved problems", LightPurple
	}

	if mr.numProblems == 0 {
		return "Pending", LightYellow
	} else {
		return "Problems resolved", LightYellow
	}
}
