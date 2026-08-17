// Package jobstore is a Redis-backed, cluster-wide store of scheduled job
// definitions. Every instance in a cluster reads from the same Redis hash, so
// a single write (add/update/delete) converges the whole cluster.
package jobstore

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/redis/go-redis/v9"
)

const jobsKey = "cron:jobs"

type JobType string

const (
	JobTypeFunc JobType = "func"
	JobTypeHTTP JobType = "http"
)

type HTTPConfig struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	Body   string `json:"body"`
}

// JobDef is the shared, cluster-wide definition of a scheduled job.
// It lives in Redis (one hash) so every instance converges to the same set.
type JobDef struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Type        JobType    `json:"type"` // "func" or "http"
	Schedule    string     `json:"schedule"`
	WithSeconds bool       `json:"with_seconds"` // true => 6-field cron (with seconds)
	Enabled     bool       `json:"enabled"`
	Func        string     `json:"func,omitempty"` // used when Type == "func"
	HTTP        HTTPConfig `json:"http,omitempty"` // used when Type == "http"
}

type Store struct {
	client *redis.Client
}

// New constructs a Store backed by the given Redis client.
func New(client *redis.Client) *Store {
	return &Store{client: client}
}

func (s *Store) List(ctx context.Context) (map[string]JobDef, error) {
	res, err := s.client.HGetAll(ctx, jobsKey).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]JobDef, len(res))
	for id, raw := range res {
		var d JobDef
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			continue
		}
		out[id] = d
	}
	return out, nil
}

func (s *Store) Get(ctx context.Context, id string) (JobDef, bool, error) {
	raw, err := s.client.HGet(ctx, jobsKey, id).Result()
	if errors.Is(err, redis.Nil) {
		return JobDef{}, false, nil
	}
	if err != nil {
		return JobDef{}, false, err
	}
	var d JobDef
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return JobDef{}, false, err
	}
	return d, true, nil
}

func (s *Store) Put(ctx context.Context, d JobDef) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return s.client.HSet(ctx, jobsKey, d.ID, raw).Err()
}

func (s *Store) Delete(ctx context.Context, id string) error {
	return s.client.HDel(ctx, jobsKey, id).Err()
}

// SeedIfEmpty populates the store with defs only when it is empty,
// so the demo shows activity out of the box without clobbering user edits.
func (s *Store) SeedIfEmpty(ctx context.Context, defs []JobDef) error {
	n, err := s.client.HLen(ctx, jobsKey).Result()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	for _, d := range defs {
		if err := s.Put(ctx, d); err != nil {
			return err
		}
	}
	return nil
}
