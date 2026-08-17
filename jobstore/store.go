// Package jobstore defines the cluster-wide store of scheduled job
// definitions. It is interface-based so any backend (Redis, MySQL, in-memory,
// ...) can be plugged in. Every instance in a cluster reads from the same
// store, so a single write (add/update/delete) converges the whole cluster.
package jobstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by implementations when a requested key does not
// exist. Get implementations should instead return ok=false; ErrNotFound is
// provided for advanced callers that need to distinguish "missing" from other
// failures in a scan-like context.
var ErrNotFound = errors.New("jobstore: not found")

// Store is the persistence interface used by the scheduler, executor and admin
// layers. Implementations must be safe for concurrent use from multiple
// goroutines and instances.
type Store interface {
	// List returns all job definitions keyed by ID.
	List(ctx context.Context) (map[string]JobDef, error)
	// Get returns the job with the given ID; ok is false when it does not exist.
	Get(ctx context.Context, id string) (JobDef, bool, error)
	// Put creates or overwrites a job definition (upsert by ID).
	Put(ctx context.Context, d JobDef) error
	// Delete removes the job with the given ID.
	Delete(ctx context.Context, id string) error
	// OnExecuted records the result of a single job execution. Implementations
	// keep only the most recent result per job (overwrite semantics). The
	// executor calls this once after every run (on the leader instance).
	OnExecuted(ctx context.Context, rec ExecutionRecord) error
	// LastExecution returns the most recent execution record for the job, or
	// ok=false when none has been recorded yet.
	LastExecution(ctx context.Context, jobID string) (ExecutionRecord, bool, error)
}

// ExecutionRecord describes the outcome of one job execution. It is recorded by
// the executor via Store.OnExecuted and can be retrieved through
// Store.LastExecution or the admin API. Only the latest record per job is kept.
type ExecutionRecord struct {
	JobID      string    `json:"job_id"`      // matches JobDef.ID
	JobName    string    `json:"job_name"`    // denormalized for display
	Instance   string    `json:"instance"`    // instanceID that ran the job (the leader)
	StartedAt  time.Time `json:"started_at"`  // when the run began
	FinishedAt time.Time `json:"finished_at"` // when the run completed
	Success    bool      `json:"success"`     // true when the run succeeded
	Error      string    `json:"error,omitempty"`   // failure reason; empty on success
	HTTPStatus int       `json:"http_status,omitempty"` // HTTP status; 0 for func jobs
	HTTPBody   string    `json:"http_body,omitempty"`   // HTTP response body (truncated)
}

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
// It lives in the store so every instance converges to the same set.
type JobDef struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Type        JobType    `json:"type"` // "func" or "http"
	Schedule    string     `json:"schedule"`
	WithSeconds bool       `json:"with_seconds"` // true => 6-field cron (with seconds)
	Enabled     bool       `json:"enabled"`
	Func        string     `json:"func,omitempty"` // used when Type == "func"
	HTTP        HTTPConfig `json:"http"`           // used when Type == "http"
}

// RedisStore is the default, Redis-backed implementation of Store.
// Job definitions are kept in a single hash.
type RedisStore struct {
	client *redis.Client
	prefix string // Redis key prefix to avoid collisions across systems
}

// New constructs a RedisStore backed by the given Redis client. The default
// key prefix "agcron" is used (keys look like "agcron:jobs").
func New(client *redis.Client) *RedisStore {
	return NewWithPrefix(client, "agcron")
}

// NewWithPrefix is like New but lets you namespace all Redis keys under prefix
// (e.g. "myapp") so multiple systems can share one Redis DB without clashes.
// An empty prefix falls back to "cron".
func NewWithPrefix(client *redis.Client, prefix string) *RedisStore {
	if prefix == "" {
		prefix = "cron"
	}
	return &RedisStore{client: client, prefix: prefix}
}

// jobsKey returns the hash key for job definitions under this store's prefix.
func (s *RedisStore) jobsKey() string { return s.prefix + ":jobs" }

// execKey returns the hash key for last executions under this store's prefix.
func (s *RedisStore) execKey() string { return s.prefix + ":executions" }

// Ensure RedisStore satisfies the Store interface at compile time.
var _ Store = (*RedisStore)(nil)

func (s *RedisStore) List(ctx context.Context) (map[string]JobDef, error) {
	res, err := s.client.HGetAll(ctx, s.jobsKey()).Result()
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

func (s *RedisStore) Get(ctx context.Context, id string) (JobDef, bool, error) {
	raw, err := s.client.HGet(ctx, s.jobsKey(), id).Result()
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

func (s *RedisStore) Put(ctx context.Context, d JobDef) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return s.client.HSet(ctx, s.jobsKey(), d.ID, raw).Err()
}

func (s *RedisStore) Delete(ctx context.Context, id string) error {
	return s.client.HDel(ctx, s.jobsKey(), id).Err()
}

func (s *RedisStore) OnExecuted(ctx context.Context, rec ExecutionRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.client.HSet(ctx, s.execKey(), rec.JobID, raw).Err()
}

func (s *RedisStore) LastExecution(ctx context.Context, jobID string) (ExecutionRecord, bool, error) {
	raw, err := s.client.HGet(ctx, s.execKey(), jobID).Result()
	if errors.Is(err, redis.Nil) {
		return ExecutionRecord{}, false, nil
	}
	if err != nil {
		return ExecutionRecord{}, false, err
	}
	var rec ExecutionRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return ExecutionRecord{}, false, err
	}
	return rec, true, nil
}

// SeedIfEmpty populates the store with defs only when it is empty, so the demo
// shows activity out of the box without clobbering user edits. It works with
// any Store implementation.
func SeedIfEmpty(ctx context.Context, s Store, defs []JobDef) error {
	all, err := s.List(ctx)
	if err != nil {
		return err
	}
	if len(all) > 0 {
		return nil
	}
	for _, d := range defs {
		if err := s.Put(ctx, d); err != nil {
			return err
		}
	}
	return nil
}
