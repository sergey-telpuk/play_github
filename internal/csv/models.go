package csv

import "strconv"

const (
	PullRequestEvent = "PullRequestEvent"
	PushEvent        = "PushEvent"
	WatchEvent       = "WatchEvent"
)

type Event struct {
	ID     int64
	Type   string
	UserID int64
	RepoID int64
}

func NewEventFromSlice(rec []string) *Event {
	id, _ := strconv.ParseInt(rec[0], 10, 64)
	userID, _ := strconv.ParseInt(rec[2], 10, 64)
	repoID, _ := strconv.ParseInt(rec[3], 10, 64)

	return &Event{
		ID:     id,
		Type:   rec[1],
		UserID: userID,
		RepoID: repoID,
	}
}

type Repo struct {
	ID   int64
	Name string
}

func NewRepoFromSlice(rec []string) *Repo {
	id, _ := strconv.ParseInt(rec[0], 10, 64)

	return &Repo{
		ID:   id,
		Name: rec[1],
	}
}

type User struct {
	ID       int64
	Username string
}

func NewUserFromSlice(rec []string) *User {
	id, _ := strconv.ParseInt(rec[0], 10, 64)

	return &User{
		ID:       id,
		Username: rec[1],
	}
}

type UserPullRequestEventAndPushEventCounter struct {
	PullRequestCounter int64
	PushCounter        int64
	UserID             int64
	Username           string
}

type ReposCommitsCounter struct {
	Counter int64
	Name    string
}
