package elector

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var errNotLeader = errors.New("not leader")

// RedisElector implements gocron's Elector interface (IsLeader(context.Context) error)
// using a Redis lock (SET NX + heartbeat renewal). Only the instance holding the lock
// is the leader and runs jobs; followers stay warm and take over if the leader dies.
type RedisElector struct {
	client *redis.Client
	key    string
	id     string
	ttl    time.Duration

	mu       sync.RWMutex
	isLeader bool

	stopCh chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
}

func New(client *redis.Client, key, id string, ttl time.Duration) *RedisElector {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	e := &RedisElector{
		client: client,
		key:    key,
		id:     id,
		ttl:    ttl,
		stopCh: make(chan struct{}),
	}
	e.wg.Add(1)
	go e.campaign()
	return e
}

func (e *RedisElector) campaign() {
	defer e.wg.Done()
	e.try()
	ticker := time.NewTicker(e.ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.try()
		}
	}
}

func (e *RedisElector) try() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ok, err := e.client.SetNX(ctx, e.key, e.id, e.ttl).Result()
	if err != nil {
		e.setLeader(false)
		return
	}
	if ok {
		e.setLeader(true)
		return
	}
	// Lock already held: if it is ours, renew it; otherwise we are not leader.
	val, err := e.client.Get(ctx, e.key).Result()
	if err == nil && val == e.id {
		e.client.PExpire(ctx, e.key, e.ttl)
		e.setLeader(true)
		return
	}
	e.setLeader(false)
}

func (e *RedisElector) setLeader(v bool) {
	e.mu.Lock()
	if e.isLeader != v {
		e.isLeader = v
		if v {
			log.Printf("[elector %s] became LEADER", e.id)
		} else {
			log.Printf("[elector %s] lost leadership", e.id)
		}
	}
	e.mu.Unlock()
}

// IsLeader implements gocron's Elector interface.
func (e *RedisElector) IsLeader(_ context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.isLeader {
		return nil
	}
	return errNotLeader
}

// IsLeaderNow is a convenience for the /status endpoint.
func (e *RedisElector) IsLeaderNow() bool {
	return e.IsLeader(context.Background()) == nil
}

// Close stops the campaign loop and releases the lock if we currently hold it.
func (e *RedisElector) Close() {
	e.once.Do(func() {
		close(e.stopCh)
		ctx := context.Background()
		val, err := e.client.Get(ctx, e.key).Result()
		if err == nil && val == e.id {
			e.client.Del(ctx, e.key)
		}
		e.wg.Wait()
	})
}
