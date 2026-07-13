package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"crondemo/internal/admin"
	"crondemo/internal/elector"
	"crondemo/internal/executor"
	"crondemo/internal/jobstore"
	"crondemo/internal/scheduler"
)

func main() {
	instID := os.Getenv("INSTANCE_ID")
	if instID == "" {
		instID, _ = os.Hostname()
	}
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	redisPass := envOr("REDIS_PASS", "redis_JPtEYa")
	electorKey := envOr("ELECTOR_KEY", "cron:leader")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: redisPass})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	store := jobstore.New(rdb)
	if err := store.SeedIfEmpty(context.Background(), exampleJobs()); err != nil {
		log.Fatalf("seed jobs failed: %v", err)
	}

	funcs := executor.FuncRegistry{
		"sayHello": func(ctx context.Context, j jobstore.JobDef) error {
			log.Printf("[func sayHello] job %q executed", j.Name)
			return nil
		},
		"reportStatus": func(ctx context.Context, j jobstore.JobDef) error {
			log.Printf("[func reportStatus] job %q executed", j.Name)
			return nil
		},
	}

	exec := executor.New(store, instID, funcs)
	e := elector.New(rdb, electorKey, instID, 10*time.Second)

	sched, err := scheduler.New(store, exec, e, 5*time.Second)
	if err != nil {
		log.Fatalf("create scheduler failed: %v", err)
	}
	sched.Start()

	api := admin.New(store, instID, e, sched)
	go func() {
		log.Printf("[%s] admin/status on :8080", instID)
		if err := http.ListenAndServe(":8080", api.Mux()); err != nil {
			log.Printf("http server error: %v", err)
		}
	}()

	log.Printf("[%s] started; leader election key=%s", instID, electorKey)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("[%s] shutting down", instID)
	sched.Stop()
	e.Close()
	_ = rdb.Close()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func exampleJobs() []jobstore.JobDef {
	return []jobstore.JobDef{
		{
			ID:          "job-hello",
			Name:        "say-hello",
			Type:        jobstore.JobTypeFunc,
			Schedule:    "*/10 * * * * *",
			WithSeconds: true,
			Enabled:     true,
			Func:        "sayHello",
		},
		{
			ID:          "job-http",
			Name:        "ping-echo",
			Type:        jobstore.JobTypeHTTP,
			Schedule:    "*/15 * * * * *",
			WithSeconds: true,
			Enabled:     true,
			HTTP:        jobstore.HTTPConfig{Method: "POST", URL: "http://localhost:8080/echo", Body: `{"from":"cron-demo"}`},
		},
	}
}
