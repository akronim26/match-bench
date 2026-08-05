// Package main starts the submission-api service.
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
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/iicpc/libs/authn"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
	"github.com/iicpc/submission-api/internal/consumer"
	"github.com/iicpc/submission-api/internal/handler"
	"github.com/iicpc/submission-api/internal/publisher"
	"github.com/iicpc/submission-api/internal/scenarios"
	"github.com/iicpc/submission-api/internal/store"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "submission-api"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)

	if lokiClient != nil {
		defer lokiClient.Close()
	}

	port := envOr("PORT", "8080")
	dbURL := mustEnv("DATABASE_URL")
	minioEndpoint := mustEnv("MINIO_ENDPOINT")
	minioAccess := mustEnv("MINIO_ACCESS_KEY")
	minioSecret := mustEnv("MINIO_SECRET_KEY")
	minioBucket := envOr("MINIO_BUCKET", "submissions")
	minioSSL := os.Getenv("MINIO_USE_SSL") == "true"
	minioCreateBucket := os.Getenv("MINIO_CREATE_BUCKET_IF_MISSING") == "true"
	kafkaBrokers := mustEnv("KAFKA_BROKERS")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	minioInitCtx, minioCancel := context.WithTimeout(ctx, 5*time.Second)
	minioStore, err := store.NewMinioStoreWithOptions(
		minioInitCtx,
		minioEndpoint,
		minioAccess,
		minioSecret,
		minioBucket,
		minioSSL,
		store.MinioStoreOptions{CreateBucketIfMissing: minioCreateBucket},
	)
	minioCancel()
	if err != nil {
		log.Error("minio init failed", "error", err)
		os.Exit(1)
	}

	pgStore, err := store.NewPostgresStore(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			pgStore.RecordPoolStats()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	scenarioCfg := scenarios.ConfigFromEnv()
	reseed := envBool("RESEED_SCENARIOS", false)
	scenarioRows, err := scenarios.BuildAll(scenarioCfg)
	if err != nil {
		log.Error("build scenarios failed", "error", err)
		os.Exit(1)
	}
	// SEED_SCENARIOS optionally restricts which built scenarios are seeded (CSV of
	// names, e.g. "constant"). Empty = seed all. Note SeedScenarios only inserts/
	// upserts, so pre-existing rows for excluded scenarios must be deleted once via
	// SQL — they are simply never re-created here after that.
	if only := envOr("SEED_SCENARIOS", ""); strings.TrimSpace(only) != "" {
		scenarioRows = filterScenarios(scenarioRows, only)
		log.Info("scenario seed filtered", "keep", only, "count", len(scenarioRows))
	}
	storeRows := make([]store.ScenarioRow, len(scenarioRows))
	for i, sr := range scenarioRows {
		storeRows[i] = store.ScenarioRow{
			ScenarioID: sr.ScenarioID,
			Name:       sr.Name,
			SortOrder:  sr.SortOrder,
			DurationNs: sr.DurationNs,
			TaskSpecs:  sr.TaskSpecs,
		}
	}
	if err := pgStore.SeedScenarios(ctx, storeRows, reseed); err != nil {
		log.Error("seed scenarios failed", "error", err)
		os.Exit(1)
	}
	log.Info("scenarios seeded", "count", len(storeRows), "reseed", reseed,
		"constant_rps", scenarioCfg.ConstantTotalRPS, "spike_peak_rps", scenarioCfg.SpikePeakRPS,
		"constant_duration", scenarioCfg.ConstantDuration.String())

	kafkaPub := publisher.NewKafkaPublisher(kafkaBrokers, log)
	defer kafkaPub.Close()

	group := envOr("KAFKA_BENCHMARK_STATUS_GROUP", "submission-api-benchmark-status")
	benchStatusConsumer := consumer.NewBenchmarkStatusConsumer(kafkaBrokers, group, pgStore, kafkaPub, log)
	defer benchStatusConsumer.Close()
	go benchStatusConsumer.Start(ctx)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(metrics.HTTPMiddleware("submission-api", chiRoutePattern))
	r.Use(middleware.Recoverer)

	healthHandler, err := handler.Health(log)
	if err != nil {
		log.Error("health handler init failed", "error", err)
		os.Exit(1)
	}

	r.Get("/health", healthHandler)
	r.Get("/ready", handler.Readiness(pgStore.Ping, log))
	r.Handle("/metrics", metrics.Handler())

	var authMW func(http.Handler) http.Handler
	if envBool("AUTH_REQUIRED", true) {
		googleClientID := mustEnv("GOOGLE_CLIENT_ID")
		verifier, err := authn.NewVerifier(googleClientID)
		if err != nil {
			log.Error("token verifier init failed", "error", err)
			os.Exit(1)
		}
		authMW = handler.RequireContestant(verifier, log)
	} else {
		// AUTH_REQUIRED=false → authentication disabled entirely: no bearer token is
		// required. The caller's contestant identity is taken from an (unverified)
		// token sub claim if present, otherwise DEFAULT_CONTESTANT_ID. Dev/e2e only.
		defaultContestant := getenvOr("DEFAULT_CONTESTANT_ID", "e2e-contestant")
		log.Warn("AUTH_REQUIRED=false — authentication DISABLED; using default contestant when no token",
			"default_contestant_id", defaultContestant)
		authMW = handler.OptionalContestant(defaultContestant, log)
	}

	r.Group(func(r chi.Router) {
		r.Use(authMW)
		r.Post("/submit", handler.Submit(minioStore, pgStore, kafkaPub, log))
		r.Get("/submissions/{submission_id}", handler.GetSubmission(pgStore, log))
		r.Post("/submissions/{submission_id}/benchmark", handler.StartBenchmark(pgStore, kafkaPub, log))
		r.Post("/benchmarks/{submission_id}", handler.StartBenchmark(pgStore, kafkaPub, log))
		r.Get("/run-groups", handler.ListRunGroups(pgStore, log))
		r.Get("/run-groups/{run_group_id}", handler.GetRunGroup(pgStore, log))
		r.Get("/runs/{session_id}", handler.GetRun(pgStore, log))
	})

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("server started", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "error", err)
	}
	log.Info("server stopped")
}

// chiRoutePattern performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func chiRoutePattern(r *http.Request) string {
	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		return routeCtx.RoutePattern()
	}
	return ""
}

// requestLogger performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			ctx := logger.WithAttrs(r.Context(),
				slog.String("request_id", middleware.GetReqID(r.Context())),
				slog.String("http_method", r.Method),
				slog.String("http_path", r.URL.Path),
				slog.String("remote_addr", r.RemoteAddr),
				slog.String("user_agent", r.UserAgent()),
			)
			next.ServeHTTP(ww, r.WithContext(ctx))

			log.InfoContext(ctx, "http request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// filterScenarios keeps only the scenarios whose name is in the CSV allowlist.
// It preserves input order and silently drops unknown names, so a bad entry
// yields fewer rows rather than a startup failure.
func filterScenarios(rows []scenarios.ScenarioRow, csv string) []scenarios.ScenarioRow {
	keep := make(map[string]bool)
	for _, n := range strings.Split(csv, ",") {
		if n = strings.TrimSpace(n); n != "" {
			keep[n] = true
		}
	}
	out := rows[:0:0]
	for _, r := range rows {
		if keep[r.Name] {
			out = append(out, r)
		}
	}
	return out
}

// envBool performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envBool(key string, def bool) bool {
	switch os.Getenv(key) {
	case "true", "1", "TRUE", "True":
		return true
	case "false", "0", "FALSE", "False":
		return false
	default:
		return def
	}
}

// getenvOr returns the env var value for key, or def when it is unset/empty.
func getenvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// mustEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "var", key)
		os.Exit(1)
	}
	return v
}
