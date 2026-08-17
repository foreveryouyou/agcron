// Command mysql shows how to swap the default Redis job store for a custom
// implementation — here a MySQL-backed one. Leader election still uses Redis;
// only the job definitions move to MySQL.
//
// Prerequisite: a MySQL schema with the job table. Create it once via:
//
//	dsn := "root:pass@tcp(localhost:3306)/cron?parseTime=true"
//	db, _ := sql.Open("mysql", dsn)
//	store := mysqlstore.New(db, "cron_jobs")
//	store.Migrate(ctx)
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/foreveryouyou/agcron"
	"github.com/foreveryouyou/agcron/executor"
	"github.com/foreveryouyou/agcron/jobstore"
	"github.com/foreveryouyou/agcron/jobstore/mysqlstore"
)

func main() {
	instID := os.Getenv("INSTANCE_ID")
	if instID == "" {
		instID, _ = os.Hostname()
	}

	ctx := context.Background()

	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		dsn = "agcron:agcrondemo@tcp(localhost:3306)/agcron?parseTime=true"
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open mysql failed: %v", err)
	}
	defer db.Close()

	store := mysqlstore.New(db, "cron_jobs")
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("migrate mysql store failed: %v", err)
	}

	eng, err := agcron.New(ctx, agcron.Config{
		RedisAddr:  envOr("REDIS_ADDR", "localhost:6379"),
		RedisPass:  envOr("REDIS_PASS", ""),
		InstanceID: instID,
		KeyPrefix:  envOr("KEY_PREFIX", ""), // namespaces all Redis keys; share one Redis DB safely
		ElectorTTL: 10 * time.Second,
		Reconcile:  5 * time.Second,
		Store:      store, // custom store replaces the default Redis store
		AdminAddr:  ":8081",
		Funcs: executor.FuncRegistry{
			"sayHello": func(ctx context.Context, j jobstore.JobDef) error {
				log.Printf("[func sayHello] job %q executed", j.Name)
				return nil
			},
		},
		Seed: []jobstore.JobDef{
			{
				ID:          "job-hello-mysql",
				Name:        "say-hello-mysql",
				Type:        jobstore.JobTypeFunc,
				Schedule:    "*/10 * * * * *",
				WithSeconds: true,
				Enabled:     true,
				Func:        "sayHello",
			},
		},
	})
	if err != nil {
		log.Fatalf("start engine failed: %v", err)
	}

	eng.Start()
	log.Printf("[%s] started with mysql store; admin on :8081", instID)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("[%s] shutting down", instID)
	eng.Stop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
