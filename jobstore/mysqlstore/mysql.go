// Package mysqlstore provides a MySQL-backed implementation of jobstore.Store.
// It is a drop-in replacement for the default Redis store, for teams that
// prefer keeping job definitions in a relational database.
package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	// Register the MySQL driver.
	_ "github.com/go-sql-driver/mysql"

	"github.com/foreveryouyou/agcron/jobstore"
)

// Store is a MySQL implementation of jobstore.Store. Job definitions live in
// a single table, one row per job.
type Store struct {
	db    *sql.DB
	table string
}

// New wraps an existing *sql.DB. Caller owns the connection lifecycle.
// Example:
//
//	dsn := "user:pass@tcp(127.0.0.1:3306)/cron?parseTime=true&charset=utf8mb4"
//	db, _ := sql.Open("mysql", dsn)
//	store := mysqlstore.New(db, "cron_jobs")
func New(db *sql.DB, table string) *Store {
	if table == "" {
		table = "cron_jobs"
	}
	return &Store{db: db, table: table}
}

// Open opens a MySQL connection and returns a Store backed by it.
func Open(dsn string, table string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysqlstore: open: %w", err)
	}
	return New(db, table), nil
}

// Ensure Store satisfies the jobstore.Store interface at compile time.
var _ jobstore.Store = (*Store)(nil)

// DDL is the recommended table schema for job definitions.
// For tables created by an older version (before the shell column existed),
// run: ALTER TABLE cron_jobs ADD COLUMN shell JSON NULL AFTER http;
const DDL = `
CREATE TABLE IF NOT EXISTS cron_jobs (
    id           VARCHAR(128) NOT NULL,
    name         VARCHAR(255) NOT NULL DEFAULT '',
    type         VARCHAR(16)  NOT NULL,
    schedule     VARCHAR(128) NOT NULL,
    with_seconds TINYINT(1)   NOT NULL DEFAULT 0,
    enabled      TINYINT(1)   NOT NULL DEFAULT 1,
    func_name    VARCHAR(255) NOT NULL DEFAULT '',
    http         JSON         NULL,
    shell        JSON         NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// execDDL is the schema for the last-execution table. It is kept separate from
// cron_jobs so existing deployments are not forced to migrate their job table.
const execDDL = `
CREATE TABLE IF NOT EXISTS cron_executions (
    job_id       VARCHAR(128) NOT NULL,
    job_name     VARCHAR(255) NOT NULL DEFAULT '',
    instance     VARCHAR(255) NOT NULL DEFAULT '',
    started_at   DATETIME(3)  NOT NULL,
    finished_at  DATETIME(3)  NOT NULL,
    success      TINYINT(1)   NOT NULL DEFAULT 0,
    error        TEXT         NULL,
    http_status  INT          NOT NULL DEFAULT 0,
    result       TEXT         NULL,
    PRIMARY KEY (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// Migrate creates the job table (and the executions table) if they do not exist.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, DDL); err != nil {
		return fmt.Errorf("mysqlstore: migrate jobs: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, execDDL); err != nil {
		return fmt.Errorf("mysqlstore: migrate executions: %w", err)
	}
	return nil
}

func (s *Store) List(ctx context.Context) (map[string]jobstore.JobDef, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT id, name, type, schedule, with_seconds, enabled, func_name, http, shell FROM "+s.table)
	if err != nil {
		return nil, fmt.Errorf("mysqlstore: list: %w", err)
	}
	defer rows.Close()

	out := make(map[string]jobstore.JobDef)
	for rows.Next() {
		d, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out[d.ID] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysqlstore: list rows: %w", err)
	}
	return out, nil
}

func (s *Store) Get(ctx context.Context, id string) (jobstore.JobDef, bool, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, name, type, schedule, with_seconds, enabled, func_name, http, shell FROM "+s.table+" WHERE id = ?", id)

	var d jobstore.JobDef
	err := scanJobRow(row, &d)
	if errors.Is(err, sql.ErrNoRows) {
		return jobstore.JobDef{}, false, nil
	}
	if err != nil {
		return jobstore.JobDef{}, false, err
	}
	return d, true, nil
}

func (s *Store) Put(ctx context.Context, d jobstore.JobDef) error {
	httpRaw, err := json.Marshal(d.HTTP)
	if err != nil {
		return fmt.Errorf("mysqlstore: marshal http: %w", err)
	}
	shellRaw, err := json.Marshal(d.Shell)
	if err != nil {
		return fmt.Errorf("mysqlstore: marshal shell: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO `+s.table+`
		 (id, name, type, schedule, with_seconds, enabled, func_name, http, shell)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		 name = VALUES(name), type = VALUES(type), schedule = VALUES(schedule),
		 with_seconds = VALUES(with_seconds), enabled = VALUES(enabled),
		 func_name = VALUES(func_name), http = VALUES(http), shell = VALUES(shell)`,
		d.ID, d.Name, string(d.Type), d.Schedule, d.WithSeconds, d.Enabled, d.Func, httpRaw, shellRaw)
	if err != nil {
		return fmt.Errorf("mysqlstore: put %q: %w", d.ID, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM "+s.table+" WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("mysqlstore: delete %q: %w", id, err)
	}
	return nil
}

func (s *Store) OnExecuted(ctx context.Context, rec jobstore.ExecutionRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cron_executions
		 (job_id, job_name, instance, started_at, finished_at, success, error, http_status, result)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		 job_name = VALUES(job_name), instance = VALUES(instance),
		 started_at = VALUES(started_at), finished_at = VALUES(finished_at),
		 success = VALUES(success), error = VALUES(error),
		 http_status = VALUES(http_status), result = VALUES(result)`,
		rec.JobID, rec.JobName, rec.Instance,
		rec.StartedAt, rec.FinishedAt, rec.Success, nullString(rec.Error),
		rec.HTTPStatus, nullString(rec.Result))
	if err != nil {
		return fmt.Errorf("mysqlstore: on_executed %q: %w", rec.JobID, err)
	}
	return nil
}

func (s *Store) LastExecution(ctx context.Context, jobID string) (jobstore.ExecutionRecord, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT job_id, job_name, instance, started_at, finished_at, success, error, http_status, result
		 FROM cron_executions WHERE job_id = ?`, jobID)

	var (
		rec     jobstore.ExecutionRecord
		success bool
		errStr  sql.NullString
		resStr  sql.NullString
	)
	if err := row.Scan(&rec.JobID, &rec.JobName, &rec.Instance,
		&rec.StartedAt, &rec.FinishedAt, &success, &errStr, &rec.HTTPStatus, &resStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return jobstore.ExecutionRecord{}, false, nil
		}
		return jobstore.ExecutionRecord{}, false, fmt.Errorf("mysqlstore: last_execution %q: %w", jobID, err)
	}
	rec.Success = success
	rec.Error = errStr.String
	rec.Result = resStr.String
	return rec, true, nil
}

// nullString wraps a Go string into a sql.NullString (empty => NULL).
func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(rs rowScanner) (jobstore.JobDef, error) {
	var d jobstore.JobDef
	if err := scanJobRow(rs, &d); err != nil {
		return jobstore.JobDef{}, err
	}
	return d, nil
}

func scanJobRow(rs rowScanner, d *jobstore.JobDef) error {
	var (
		typ      string
		funcName string
		httpRaw  []byte
		shellRaw []byte
	)

	err := rs.Scan(&d.ID, &d.Name, &typ, &d.Schedule, &d.WithSeconds, &d.Enabled, &funcName, &httpRaw, &shellRaw)
	if err != nil {
		return err // callers translate sql.ErrNoRows into ok=false
	}

	d.Type = jobstore.JobType(typ)
	d.Func = funcName

	// A NULL JSON column surfaces as an empty byte slice; JSON `null` unmarshals
	// to the zero value of HTTPConfig/ShellConfig. Either way we end up with a
	// sane default.
	if len(httpRaw) > 0 {
		if err := json.Unmarshal(httpRaw, &d.HTTP); err != nil {
			return fmt.Errorf("mysqlstore: unmarshal http: %w", err)
		}
	}
	if len(shellRaw) > 0 {
		if err := json.Unmarshal(shellRaw, &d.Shell); err != nil {
			return fmt.Errorf("mysqlstore: unmarshal shell: %w", err)
		}
	}
	return nil
}
