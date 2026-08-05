// Package main starts the leaderboard-api service.
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
	"github.com/iicpc/leaderboard-api/internal/config"
	"github.com/iicpc/leaderboard-api/internal/consumer"
	"github.com/iicpc/leaderboard-api/internal/handler"
	"github.com/iicpc/leaderboard-api/internal/live"
	"github.com/iicpc/leaderboard-api/internal/read"
	"github.com/iicpc/leaderboard-api/internal/redis"
	"github.com/iicpc/leaderboard-api/internal/sse"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "leaderboard-api"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}
	cfg := config.Load(log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	reader, err := read.New(ctx, cfg.MetadataDatabaseURL, cfg.TimescaleDatabaseURL)
	if err != nil {
		log.Error("read store init failed", "error", err)
		os.Exit(1)
	}
	defer reader.Close()
	redisClient := redis.New(cfg.RedisAddr)
	defer redisClient.Close()

	metricsSrv, err := metrics.StartServer(cfg.MetricsAddr)
	if err != nil {
		log.Error("metrics server start failed", "addr", cfg.MetricsAddr, "error", err)
	} else {
		defer metricsSrv.Close()
	}

	cachedReader := read.NewCached(reader, redisClient, 2*time.Second)
	broker := sse.New(func(ctx context.Context) (any, error) {
		return reader.Leaderboard(ctx, read.LeaderboardQuery{Limit: 100})
	})
	go consumer.New(cfg.KafkaBrokers, cfg.KafkaGroup, broker, log).Run(ctx)
	go live.New(sessionSourceAdapter{reader}, redisClient, broker, 0, log).Run(ctx)

	h := handler.New(cachedReader, cfg.PrometheusURL)
	var isReady atomic.Bool
	isReady.Store(true)

	r := chi.NewRouter()
	r.Use(metrics.HTTPMiddleware("leaderboard-api", chiRoutePattern))
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/ready", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if isReady.Load() && reader.Healthcheck(checkCtx) == nil && redisClient.Ping(checkCtx) == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if isReady.Load() && reader.Healthcheck(checkCtx) == nil && redisClient.Ping(checkCtx) == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	r.Handle("/metrics", metrics.Handler())
	r.Get("/api/leaderboard", h.Leaderboard)
	r.Get("/api/live", h.LiveRuns)
	r.Get("/api/runs/{run_group_id}", h.RunDetail)
	r.Get("/api/charts/{session_id}", h.Chart)
	r.Get("/api/health-panel", h.HealthPanel)
	r.Get("/api/events", broker.ServeHTTP)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Info("leaderboard-api started", "port", cfg.Port)
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

// sessionSourceAdapter adapts *read.Store to live.SessionSource, converting the store's
// result type to the poller-local type it depends on.
type sessionSourceAdapter struct {
	reader *read.Store
}

func (a sessionSourceAdapter) ActiveSessionContestants(ctx context.Context) ([]live.ActiveSessionContestant, error) {
	rows, err := a.reader.ActiveSessionContestants(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]live.ActiveSessionContestant, len(rows))
	for i, r := range rows {
		out[i] = live.ActiveSessionContestant{SessionID: r.SessionID, ContestantID: r.ContestantID}
	}
	return out, nil
}

// chiRoutePattern performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func chiRoutePattern(r *http.Request) string {
	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		return routeCtx.RoutePattern()
	}
	return r.URL.Path
}
