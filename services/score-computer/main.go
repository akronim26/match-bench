// Package main starts the score-computer service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/score-computer/internal/config"
	"github.com/iicpc/score-computer/internal/publisher"
	"github.com/iicpc/score-computer/internal/redis"
	"github.com/iicpc/score-computer/internal/store"
	"github.com/iicpc/score-computer/internal/trigger"
	"github.com/iicpc/score-computer/internal/worker"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "score-computer"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}

	cfg := config.Load(log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, cfg.MetadataDatabaseURL, cfg.TimescaleDatabaseURL)
	if err != nil {
		log.Error("store init failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	redisClient := redis.New(cfg.RedisAddr)
	defer redisClient.Close()

	// One-shot cleanup of the orphaned `leaderboard:global` ZSET (writer/reader
	// code removed in a prior commit, key never deleted). Redis being
	// unreachable here is tolerated the same way readyz already tolerates it
	// elsewhere — this must not block startup.
	if deleted, err := redisClient.DeleteLegacyGlobalLeaderboard(ctx); err != nil {
		log.Warn("legacy leaderboard:global cleanup skipped", "error", err)
	} else if deleted {
		log.Info("deleted legacy leaderboard:global key")
	} else {
		log.Info("legacy leaderboard:global key not found, nothing to clean up")
	}

	pub := publisher.New(cfg.KafkaBrokers)
	defer pub.Close()

	metricsSrv, err := metrics.StartServer(cfg.MetricsAddr)
	if err != nil {
		log.Error("metrics server start failed", "addr", cfg.MetricsAddr, "error", err)
	} else {
		defer metricsSrv.Close()
	}

	ready := make(chan string, cfg.Concurrency*4)
	consumer := trigger.New(cfg.KafkaBrokers, st, ready, log)
	go consumer.RunStatus(ctx, cfg.StatusGroup)
	go consumer.RunCorrectness(ctx, cfg.CorrectnessGroup)

	w := worker.New(st, redisClient, pub, log)
	for i := 0; i < cfg.Concurrency; i++ {
		go w.Run(ctx, ready)
	}

	go func() {
		pending, err := st.PendingRunGroups(ctx)
		if err != nil {
			log.Error("startup recovery scan failed", "error", err)
			return
		}
		for _, id := range pending {
			select {
			case ready <- id:
			case <-ctx.Done():
				return
			}
		}
		if len(pending) > 0 {
			log.Info("startup recovery enqueued pending run-groups", "count", len(pending))
		}
	}()

	var isReady atomic.Bool
	isReady.Store(true)
	r := chi.NewRouter()
	r.Use(metrics.HTTPMiddleware("score-computer", func(r *http.Request) string { return r.URL.Path }))
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if isReady.Load() && st.Healthcheck(checkCtx) == nil && redisClient.Ping(checkCtx) == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	r.Handle("/metrics", metrics.Handler())

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Info("score-computer started", "port", cfg.Port, "concurrency", cfg.Concurrency)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	isReady.Store(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
