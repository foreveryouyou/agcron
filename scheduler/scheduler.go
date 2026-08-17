// Package scheduler wraps gocron with a reconciler loop. The reconciler polls
// the shared job store and converges this instance's local gocron scheduler
// (add / update / remove) so all instances track the same job set. Only the
// leader instance actually executes jobs (gated by the Elector).
package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"

	"github.com/foreveryouyou/agcron/elector"
	"github.com/foreveryouyou/agcron/executor"
	"github.com/foreveryouyou/agcron/jobstore"
	"github.com/foreveryouyou/agcron/logx"
)

type localEntry struct {
	uid         uuid.UUID
	schedule    string
	withSeconds bool
}

// Scheduler wraps a gocron scheduler plus a reconciler loop.
type Scheduler struct {
	gocron   gocron.Scheduler
	store    jobstore.Store
	exec     *executor.Executor
	interval time.Duration
	log      logx.Logger

	mu    sync.Mutex
	local map[string]localEntry

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New constructs a Scheduler. interval controls how often the reconciler polls
// the store; pass <=0 for the 5s default. log is optional; a nil value uses the
// logx.Default logger.
func New(store jobstore.Store, exec *executor.Executor, e *elector.RedisElector, interval time.Duration, log logx.Logger) (*Scheduler, error) {
	g, err := gocron.NewScheduler(gocron.WithDistributedElector(e))
	if err != nil {
		return nil, err
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Scheduler{
		gocron:   g,
		store:    store,
		exec:     exec,
		interval: interval,
		log:      logx.With(log),
		local:    make(map[string]localEntry),
		stopCh:   make(chan struct{}),
	}, nil
}

func (s *Scheduler) Start() {
	s.gocron.Start()
	s.wg.Add(1)
	go s.reconcileLoop()
}

func (s *Scheduler) reconcileLoop() {
	defer s.wg.Done()
	s.reconcile()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.reconcile()
		}
	}
}

func (s *Scheduler) reconcile() {
	ctx := context.Background()
	defs, err := s.store.List(ctx)
	if err != nil {
		s.log.Errorf("[reconcile] list error: %v", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, d := range defs {
		entry, ok := s.local[id]
		if !ok {
			s.add(id, d)
			continue
		}
		if entry.schedule != d.Schedule || entry.withSeconds != d.WithSeconds {
			if err := s.gocron.RemoveJob(entry.uid); err != nil {
				s.log.Errorf("[reconcile] remove %s error: %v", id, err)
			}
			delete(s.local, id)
			s.add(id, d)
		}
	}

	for id, entry := range s.local {
		if _, ok := defs[id]; !ok {
			if err := s.gocron.RemoveJob(entry.uid); err != nil {
				s.log.Errorf("[reconcile] remove %s error: %v", id, err)
			}
			delete(s.local, id)
		}
	}
}

func (s *Scheduler) add(id string, d jobstore.JobDef) {
	task := gocron.NewTask(func() {
		s.exec.Run(context.Background(), id)
	})
	job, err := s.gocron.NewJob(
		gocron.CronJob(d.Schedule, d.WithSeconds),
		task,
		gocron.WithName(d.Name),
		gocron.WithTags(id),
	)
	if err != nil {
		s.log.Errorf("[reconcile] add job %s error: %v", id, err)
		return
	}
	s.local[id] = localEntry{uid: job.ID(), schedule: d.Schedule, withSeconds: d.WithSeconds}
	s.log.Infof("[reconcile] added job %q (%s, seconds=%v) -> %s", d.Name, d.Schedule, d.WithSeconds, job.ID())
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
	_ = s.gocron.Shutdown()
}
