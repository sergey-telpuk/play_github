package analytics

import (
	"context"
	"example.com/m/internal/csv"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Service struct {
	eventsReader csv.Reader
	usersReader  csv.Reader
	reposReader  csv.Reader
}

func NewAnalyticsService(
	eventsReader csv.Reader,
	usersReader csv.Reader,
	reposReader csv.Reader,
) Service {
	return Service{
		eventsReader: eventsReader,
		usersReader:  usersReader,
		reposReader:  reposReader,
	}
}

func (s Service) PrintTop10ActiveUsersSortedByAmountOfPRsCreatedAndCommitsPushed(ctx context.Context) error {
	var eventsMap map[string]*csv.Event
	var usersMap map[int64]*csv.User

	wg := &sync.WaitGroup{}
	chErr := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		eventsMap, err = s.readEventsToMap(ctx, csv.PullRequestEvent, csv.PushEvent)
		if err != nil {
			chErr <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		usersMap, err = s.readUsersToMap(ctx)
		if err != nil {
			chErr <- err
		}
	}()

	go func() {
		wg.Wait()
		close(chErr)
	}()

	if err := <-chErr; err != nil {
		return err
	}

	pullRequestEventAndPushEventMap := make(map[int64]*csv.UserPullRequestEventAndPushEventCounter)

	for _, value := range eventsMap {
		if _, ok := pullRequestEventAndPushEventMap[value.UserID]; !ok {
			username := "unknown"
			if _, ok := usersMap[value.UserID]; ok {
				username = usersMap[value.UserID].Username
			}
			pullRequestEventAndPushEventMap[value.UserID] = &csv.UserPullRequestEventAndPushEventCounter{
				PullRequestCounter: 0,
				PushCounter:        0,
				UserID:             value.UserID,
				Username:           username,
			}
		}

		switch value.Type {
		case csv.PullRequestEvent:
			pullRequestEventAndPushEventMap[value.UserID].PullRequestCounter += 1
		case csv.PushEvent:
			pullRequestEventAndPushEventMap[value.UserID].PushCounter += 1
		}
	}

	sortedSlice := make([]*csv.UserPullRequestEventAndPushEventCounter, 0, len(pullRequestEventAndPushEventMap))

	for _, value := range pullRequestEventAndPushEventMap {
		sortedSlice = append(sortedSlice, value)
	}

	sort.Slice(sortedSlice, func(i, j int) bool {
		if sortedSlice[i].PullRequestCounter != sortedSlice[j].PullRequestCounter {
			return sortedSlice[i].PullRequestCounter > sortedSlice[j].PullRequestCounter
		}

		return sortedSlice[i].PushCounter > sortedSlice[j].PushCounter
	})

	fmt.Println("\nTop 10 active users sorted by amount of PRs created and commits pushed")
	fmt.Println("username : PR_count : commits_count\n-----------------------------------")

	for _, value := range sortedSlice[:10] {
		fmt.Println(fmt.Sprintf("%s: %d: %d", value.Username, value.PullRequestCounter, value.PushCounter))
	}

	return nil
}

func (s Service) PrintTop10repositoriesByAmountOfCommitsPushed(ctx context.Context) error {
	var eventsMap map[string]*csv.Event
	var reposMap map[int64]*csv.Repo

	wg := &sync.WaitGroup{}
	chErr := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		eventsMap, err = s.readEventsToMap(ctx, csv.PushEvent)
		if err != nil {
			chErr <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		reposMap, err = s.readReposToMap(ctx)
		if err != nil {
			chErr <- err
		}
	}()

	go func() {
		wg.Wait()
		close(chErr)
	}()

	if err := <-chErr; err != nil {
		return err
	}

	reposCommitsCounterMap := make(map[int64]*csv.ReposCommitsCounter)

	for _, value := range eventsMap {
		name := "unknown"
		if _, ok := reposMap[value.RepoID]; ok {
			name = reposMap[value.RepoID].Name
		}

		if _, ok := reposCommitsCounterMap[value.RepoID]; !ok {
			reposCommitsCounterMap[value.RepoID] = &csv.ReposCommitsCounter{
				Counter: 0,
				Name:    name,
			}
		}

		reposCommitsCounterMap[value.RepoID].Counter += 1
	}

	sortedSlice := make([]*csv.ReposCommitsCounter, 0, len(reposCommitsCounterMap))

	for _, value := range reposCommitsCounterMap {
		sortedSlice = append(sortedSlice, value)
	}

	sort.SliceStable(sortedSlice, func(i, j int) bool {
		return sortedSlice[i].Counter > sortedSlice[j].Counter
	})

	fmt.Println("\nTop 10 repositories by amount of commits pushed (repo name, commits count)")
	fmt.Println("repo_name : commits_count\n-------------------------")

	for _, value := range sortedSlice[:10] {
		fmt.Println(fmt.Sprintf("%s: %d: ", value.Name, value.Counter))
	}

	return nil
}

func (s Service) PrintTop10repositoriesSortedByAmountOfWatchEvents(ctx context.Context) error {
	var eventsMap map[string]*csv.Event
	var reposMap map[int64]*csv.Repo

	wg := &sync.WaitGroup{}
	chErr := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		eventsMap, err = s.readEventsToMap(ctx, csv.WatchEvent)
		if err != nil {
			chErr <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		reposMap, err = s.readReposToMap(ctx)
		if err != nil {
			chErr <- err
		}
	}()

	go func() {
		wg.Wait()
		close(chErr)
	}()

	if err := <-chErr; err != nil {
		return err
	}

	reposCommitsCounterMap := make(map[int64]*csv.ReposCommitsCounter)

	for _, value := range eventsMap {
		name := "unknown"
		if _, ok := reposMap[value.RepoID]; ok {
			name = reposMap[value.RepoID].Name
		}

		if _, ok := reposCommitsCounterMap[value.RepoID]; !ok {
			reposCommitsCounterMap[value.RepoID] = &csv.ReposCommitsCounter{
				Counter: 0,
				Name:    name,
			}
		}

		reposCommitsCounterMap[value.RepoID].Counter += 1
	}

	sortedSlice := make([]*csv.ReposCommitsCounter, 0, len(reposCommitsCounterMap))

	for _, value := range reposCommitsCounterMap {
		sortedSlice = append(sortedSlice, value)
	}

	sort.SliceStable(sortedSlice, func(i, j int) bool {
		return sortedSlice[i].Counter > sortedSlice[j].Counter
	})

	fmt.Println("\nTop 10 repositories by amount of watch events (repo name, watch events count)")
	fmt.Println("repo_name : watch_count\n-------------------------")

	for _, value := range sortedSlice[:10] {
		fmt.Println(fmt.Sprintf("%s: %d: ", value.Name, value.Counter))
	}

	return nil
}

func (s Service) readEventsToMap(ctx context.Context, filterTypeEvents ...string) (map[string]*csv.Event, error) {
	eventsMap := make(map[string]*csv.Event)
	isFilterTypeEvents := len(filterTypeEvents) > 0

	ctxReader, cancelReader := context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	defer cancelReader()
	chOut, chErr := s.eventsReader.ReadConcurrently(ctxReader, 4)

	for {
		select {
		case rec, ok := <-chOut:
			if !ok {
				return eventsMap, nil
			}
			if !isFilterTypeEvents {
				eventsMap[rec[0]] = csv.NewEventFromSlice(rec)
				continue
			}

			for _, typeEvent := range filterTypeEvents {
				if typeEvent == rec[1] {
					eventsMap[rec[0]] = csv.NewEventFromSlice(rec)
				}
			}

		case err := <-chErr:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s Service) readUsersToMap(ctx context.Context) (map[int64]*csv.User, error) {
	usersMap := make(map[int64]*csv.User)

	ctxReader, cancelReader := context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	defer cancelReader()
	chOut, chErr := s.usersReader.ReadConcurrently(ctxReader, 4)

	for {
		select {
		case rec, ok := <-chOut:
			if !ok {
				return usersMap, nil
			}
			user := csv.NewUserFromSlice(rec)
			usersMap[user.ID] = user
		case err := <-chErr:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s Service) readReposToMap(ctx context.Context) (map[int64]*csv.Repo, error) {
	reposMap := make(map[int64]*csv.Repo)

	ctxReader, cancelReader := context.WithTimeout(context.Background(), time.Duration(60)*time.Second)
	defer cancelReader()
	chOut, chErr := s.reposReader.ReadConcurrently(ctxReader, 4)

	for {
		select {
		case rec, ok := <-chOut:
			if !ok {
				return reposMap, nil
			}
			repo := csv.NewRepoFromSlice(rec)
			reposMap[repo.ID] = repo
		case err := <-chErr:
			if err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
