// Package main starts the bot-fleet-controller service.
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
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/iicpc/bot-fleet-controller/internal/controller"
	"github.com/iicpc/bot-fleet-controller/internal/handler"
	"github.com/iicpc/bot-fleet-controller/internal/orchestrator"
	"github.com/iicpc/bot-fleet-controller/internal/store"
	"github.com/iicpc/libs/logger"
	"github.com/iicpc/libs/metrics"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	logCfg := logger.DefaultConfig()
	logCfg.ServiceName = "bot-fleet-controller"
	log, lokiClient := logger.NewProductionLogger(logCfg)
	slog.SetDefault(log)
	if lokiClient != nil {
		defer lokiClient.Close()
	}

	port := envOr("PORT", "8080")
	dbURL := mustEnv("DATABASE_URL")
	kafkaBrokers := mustEnv("KAFKA_BROKERS")
	benchmarkGroup := envOr("KAFKA_BENCHMARK_GROUP", "bot-fleet-controller")
	botReadyGroup := envOr("KAFKA_BOT_READY_GROUP", "bot-fleet-controller-ready")
	orchURL := mustEnv("SANDBOX_ORCHESTRATOR_URL")
	runConfig := runConfigFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, dbURL)
	if err != nil {
		log.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			st.RecordPoolStats()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	producer := controller.NewProducer(kafkaBrokers, log)
	defer producer.Close()

	if err := controller.RecoverInFlightRuns(ctx, st, producer, log); err != nil {
		log.Error("startup recovery failed", "error", err)
		os.Exit(1)
	}

	partitionCtx, partitionCancel := context.WithTimeout(ctx, 30*time.Second)
	workloadPartitions, err := producer.WorkloadPartitionCount(partitionCtx)
	partitionCancel()
	if err != nil {
		log.Error("read workload.assignments partition count failed", "error", err)
		os.Exit(1)
	}
	leases := controller.NewPartitionLeaseAllocator(workloadPartitions)

	// Order bands are leased 1:1 with MAX_CONCURRENT_SESSIONS, so this MUST NOT exceed
	// the number of DISTINCT bands the partition count yields:
	//
	//     MAX_CONCURRENT_SESSIONS <= floor(orders_partitions / band_width)
	//
	// At the shipped 24 partitions and DEFAULT_PARTITION_BAND_WIDTH=6 that is
	// floor(24/6) = 4 EXCLUSIVE 6-partition bands (0-5, 6-11, 12-17, 18-23), one per
	// concurrently-running session.
	//
	// The invariant was violated while band_width was 8: floor(24/8) = 3 bands against
	// 4 leased sessions, so band 3's base = 3*8 = 24 wrapped modulo 24 back onto band
	// 0's partitions. Producer and consumer agreed on the wraparound, so traffic stayed
	// consistent but NOT exclusive — two sessions shared partitions and band-scoped
	// validation isolation silently stopped holding at the 4th session.
	//
	// Raising this ceiling means changing the arithmetic, not just this number: widen
	// orders.* beyond 24 partitions, or narrow band_width further.
	maxConcurrentSessions := envOrInt("MAX_CONCURRENT_SESSIONS", 4)
	bandLeases := controller.NewBandLeaseAllocator(maxConcurrentSessions)

	orchClient := orchestrator.NewClient(orchURL)
	sessions := controller.NewSessionManager()
	runner := controller.NewRunner(sessions, st, orchClient, producer, leases, bandLeases, runConfig, log)
	// The pre-scale gate reads the WORKER's consumer group (bot-fleet), not either of
	// the controller's own groups above: its members are the consumers that can receive
	// a workload spec. Must match KAFKA_CONSUMER_GROUP on the bot-fleet Deployment.
	if runConfig.CapacityWaitTimeout > 0 {
		workerGroup := envOr("KAFKA_WORKER_GROUP", "bot-fleet")
		runner.SetCapacityProbe(producer.GroupCapacityProbe(workerGroup))
		log.Info("pre-scale gate enabled",
			"worker_group", workerGroup,
			"timeout", runConfig.CapacityWaitTimeout.String())
	}
	consumer := controller.NewConsumerWithConcurrency(kafkaBrokers, benchmarkGroup, botReadyGroup, runner, producer, sessions, log, maxConcurrentSessions)
	defer consumer.Close()

	go consumer.StartBenchmarkRequested(ctx)
	go consumer.StartBotReady(ctx)

	ready := handler.NewReadyState()
	ready.MarkReady()

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogger(log))
	r.Use(metrics.HTTPMiddleware("bot-fleet-controller", chiRoutePattern))
	r.Use(middleware.Recoverer)
	r.Get("/healthz", handler.Healthz(sessions))
	r.Get("/readyz", handler.Readyz(ready))
	r.Handle("/metrics", metrics.Handler())

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("bot-fleet-controller started",
			"port", port,
			"orchestrator", orchURL,
			"deploy_deadline", runConfig.DeployDeadline,
			"ready_deadline", runConfig.ReadyDeadline,
			"barrier_safety_gap", runConfig.BarrierSafetyGap,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	// Join in-flight session goroutines before the deferred producer/consumer
	// Close runs: each session's ctx.Done path publishes a failure status and
	// deletes its sandbox slot on detached contexts — without this join those
	// calls race process exit and lose (orphaned contestant pods, runs stuck
	// non-terminal with no redelivery). 15s stays inside the default 30s k8s
	// termination grace period.
	if !consumer.WaitSessions(15 * time.Second) {
		log.Error("shutdown: in-flight sessions did not finish within 15s; their cleanup may be incomplete")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", "error", err)
	}
	log.Info("controller stopped")
}

// chiRoutePattern performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func chiRoutePattern(r *http.Request) string {
	if routeCtx := chi.RouteContext(r.Context()); routeCtx != nil {
		return routeCtx.RoutePattern()
	}
	return ""
}

// runConfigFromEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func runConfigFromEnv() controller.RunConfig {
	return controller.RunConfig{
		GlobalSeed:       uint64(envOrInt("GLOBAL_SEED", 42)),
		FIXVersion:       envOr("FIX_VERSION", "FIX.4.2"),
		ConnectTimeoutMS: uint64(envOrInt("CONNECT_TIMEOUT_MS", 1500)),
		WriteTimeoutMS:   uint64(envOrInt("WRITE_TIMEOUT_MS", 250)),

		DeployDeadline:   envOrDuration("DEPLOY_DEADLINE", 60*time.Second),
		ReadyDeadline:    envOrDuration("READY_DEADLINE", 30*time.Second),
		BarrierSafetyGap: envOrDuration("BARRIER_SAFETY_GAP", 500*time.Millisecond),

		MaxTasksPerWorker: envOrInt("MAX_TASKS_PER_WORKER", controller.DefaultMaxTasksPerWorker),
		// WORKER_RPS_CAPACITY defaults to 0 = throughput ceiling OFF, preserving the
		// task-count-only sharding this service shipped with. Deployments set it to
		// their MEASURED single-worker send ceiling; there is no safe universal
		// default (~50k/s local loopback vs 600-790k/s drain on EKS).
		WorkerRPSCapacity: uint64(envOrInt("WORKER_RPS_CAPACITY", 0)),

		// Pre-scale gate. 0 disables it. The default allows for KEDA's polling
		// interval plus a pod schedule, image pull and Kafka group join; it is
		// deliberately shorter than READY_DEADLINE so an under-provisioned fleet is
		// reported as a capacity shortfall rather than as a confusing partial ready
		// fan-in later in the run.
		CapacityWaitTimeout:  envOrDuration("CAPACITY_WAIT_TIMEOUT", 90*time.Second),
		CapacityPollInterval: envOrDuration("CAPACITY_POLL_INTERVAL", 2*time.Second),

		LeaseAcquireTimeout: envOrDuration("LEASE_ACQUIRE_TIMEOUT", 60*time.Second),
	}
}

// requestLogger performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
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

// envOrInt performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOrInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid integer env, using default", "var", key, "value", v, "default", def)
		return def
	}
	return n
}

// envOrDuration performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOrDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration env, using default", "var", key, "value", v, "default", def)
		return def
	}
	return d
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
