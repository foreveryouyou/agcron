// Command server is an example use of the agcron library: it starts a single
// distributed cron engine, seeds two demo jobs, and serves the admin API.
// Run multiple copies (with different INSTANCE_ID) against the same Redis to
// see leader election in action.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/foreveryouyou/agcron"
	"github.com/foreveryouyou/agcron/executor"
	"github.com/foreveryouyou/agcron/jobstore"
)

func main() {
	instID := os.Getenv("INSTANCE_ID")
	if instID == "" {
		instID, _ = os.Hostname()
	}

	eng, err := agcron.New(context.Background(), agcron.Config{
		RedisAddr:  envOr("REDIS_ADDR", "localhost:6379"),
		RedisPass:  envOr("REDIS_PASS", "redis_JPtEYa"),
		InstanceID: instID,
		KeyPrefix:  envOr("KEY_PREFIX", ""), // namespaces all Redis keys; share one Redis DB safely
		ElectorTTL: 10 * time.Second,
		Reconcile:  5 * time.Second,
		AdminAddr:  ":8080",
		Funcs: executor.FuncRegistry{
			"sayHello": func(ctx context.Context, j jobstore.JobDef) (string, error) {
				name, _ := j.FuncParam["name"].(string)
				log.Printf("[func sayHello] job %q executed, param: %v", j.Name, j.FuncParam)
				return "hello, " + name, nil
			},
			"reportStatus": func(ctx context.Context, j jobstore.JobDef) (string, error) {
				log.Printf("[func reportStatus] job %q executed", j.Name)
				return "status ok", nil
			},
		},
		Seed: exampleJobs(),
		// Logger: myCustomLogger, // optional; nil uses the built-in default logger
	})
	if err != nil {
		log.Fatalf("start engine failed: %v", err)
	}

	eng.Start()
	log.Printf("[%s] started; admin on :8080", instID)

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
			HTTP:        jobstore.HTTPConfig{Method: "POST", URL: "http://localhost:8080/echo", Body: `{"from":"agcron"}`},
		},
	}
}
