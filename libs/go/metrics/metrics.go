// Package metrics defines shared library behavior for metrics.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package metrics

import (
	"errors"
	"math"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const namespace = "iicpc"

var defaultBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900,
}

var byteBuckets = []float64{
	1024, 10 * 1024, 100 * 1024, 1024 * 1024, 5 * 1024 * 1024, 10 * 1024 * 1024, 25 * 1024 * 1024, 50 * 1024 * 1024, 100 * 1024 * 1024,
}

type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

// registry groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type registry struct {
	mu      sync.RWMutex
	prom    *prometheus.Registry
	metrics map[string]*metric
	errors  *prometheus.CounterVec
}

// metric groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type metric struct {
	name      string
	kind      metricKind
	buckets   []float64
	labelKeys []string
	counter   *prometheus.CounterVec
	gauge     *prometheus.GaugeVec
	histogram *prometheus.HistogramVec
}

// metricSpec groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type metricSpec struct {
	name      string
	help      string
	kind      metricKind
	labelKeys []string
	buckets   []float64
}

var global = newRegistry()

// newRegistry performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newRegistry() *registry {
	promRegistry := prometheus.NewRegistry()
	errs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: normalizeName("metrics_registry_errors_total"),
			Help: "Metrics samples dropped by the in-process registry.",
		},
		[]string{"reason"},
	)
	promRegistry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	promRegistry.MustRegister(errs)
	r := &registry{
		prom:    promRegistry,
		metrics: make(map[string]*metric),
		errors:  errs,
	}
	r.registerCatalog(projectMetricCatalog())
	return r
}

// Counter performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Counter(name, help string, labels map[string]string, delta float64) {
	if delta < 0 || !isFinite(delta) {
		global.recordError("invalid_counter_delta")
		return
	}
	if delta == 0 {
		return
	}
	m, values := global.metric(name, help, kindCounter, nil, labels)
	if m == nil {
		return
	}
	m.counter.WithLabelValues(values...).Add(delta)
}

// Gauge performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Gauge(name, help string, labels map[string]string, value float64) {
	if !isFinite(value) {
		global.recordError("invalid_gauge_value")
		return
	}
	m, values := global.metric(name, help, kindGauge, nil, labels)
	if m == nil {
		return
	}
	m.gauge.WithLabelValues(values...).Set(value)
}

// Histogram performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Histogram(name, help string, labels map[string]string, value float64) {
	HistogramWithBuckets(name, help, labels, value, bucketsForMetric(name))
}

// HistogramWithBuckets performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func HistogramWithBuckets(name, help string, labels map[string]string, value float64, buckets []float64) {
	if !isFinite(value) {
		global.recordError("invalid_histogram_observation")
		return
	}
	if len(buckets) == 0 {
		buckets = defaultBuckets
	}
	m, values := global.metric(name, help, kindHistogram, normalizeBuckets(buckets), labels)
	if m == nil {
		return
	}
	m.histogram.WithLabelValues(values...).Observe(value)
}

// Handler performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Handler() http.Handler {
	return promhttp.HandlerFor(global.prom, promhttp.HandlerOpts{ErrorHandling: promhttp.HTTPErrorOnError})
}

// StartServer performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func StartServer(addr string) (*http.Server, error) {
	if addr == "" {
		addr = ":9090"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			global.recordError("metrics_server_serve_error")
		}
	}()
	return srv, nil
}

// SinceSeconds performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func SinceSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// Labels performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Labels(values ...string) map[string]string {
	labels := make(map[string]string, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		labels[values[i]] = values[i+1]
	}
	return labels
}

// metric applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *registry) metric(name, help string, kind metricKind, buckets []float64, labels map[string]string) (*metric, []string) {
	name = normalizeName(name)
	sanitized, ok := sanitizedLabels(labels, kind == kindHistogram)
	if !ok {
		r.recordError("label_name_collision")
		return nil, nil
	}
	labels = sanitized
	labelKeys, labelValues := splitLabels(labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.metrics[name]; ok {
		if existing.kind != kind || !sameBuckets(existing.buckets, buckets) || !sameStrings(existing.labelKeys, labelKeys) {
			r.recordErrorLocked("metric_definition_conflict")
			return nil, nil
		}
		return existing, labelValues
	}

	r.recordErrorLocked("unregistered_metric")
	return nil, nil
}

// registerCatalog applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *registry) registerCatalog(specs []metricSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, spec := range specs {
		spec.name = normalizeName(spec.name)
		spec.labelKeys = sanitizeCatalogLabelKeys(spec.labelKeys, spec.kind == kindHistogram)
		if spec.kind == kindHistogram {
			spec.buckets = normalizeBuckets(spec.buckets)
		}
		if _, ok := r.metrics[spec.name]; ok {
			r.recordErrorLocked("catalog_duplicate_metric")
			continue
		}
		m := &metric{name: spec.name, kind: spec.kind, buckets: spec.buckets, labelKeys: spec.labelKeys}
		if r.registerMetricLocked(m, spec.help) {
			r.metrics[spec.name] = m
		}
	}
}

// registerMetricLocked applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *registry) registerMetricLocked(m *metric, help string) bool {
	switch m.kind {
	case kindCounter:
		m.counter = prometheus.NewCounterVec(prometheus.CounterOpts{Name: m.name, Help: help}, m.labelKeys)
		if !r.register(m.counter) {
			return false
		}
	case kindGauge:
		m.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: m.name, Help: help}, m.labelKeys)
		if !r.register(m.gauge) {
			return false
		}
	case kindHistogram:
		m.histogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: m.name, Help: help, Buckets: m.buckets}, m.labelKeys)
		if !r.register(m.histogram) {
			return false
		}
	}
	return true
}

// projectMetricCatalog performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func projectMetricCatalog() []metricSpec {
	return []metricSpec{
		counterSpec("active_run_group_conflicts_total", "Benchmark requests that joined an existing active run-group.", nil),
		counterSpec("benchmark_publish_failures_total", "Benchmark publish failures by topic.", []string{"topic"}),
		counterSpec("benchmark_requests_total", "Benchmark requests by result.", []string{"result"}),
		counterSpec("build_jobs_created_total", "Build jobs created by mode.", []string{"mode"}),
		counterSpec("build_jobs_failed_total", "Build jobs failed by mode.", []string{"mode"}),
		counterSpec("build_phase_total", "Build phases by phase and result.", []string{"phase", "result"}),
		histogramSpec("build_phase_duration_seconds", "Build phase duration in seconds.", []string{"phase", "result"}, defaultBuckets),
		histogramSpec("build_request_duration_seconds", "End-to-end build request duration in seconds.", []string{"result"}, defaultBuckets),
		counterSpec("build_requests_total", "Build requests by result.", []string{"result"}),
		counterSpec("controller_duplicate_benchmark_requested_total", "Duplicate benchmark.requested messages ignored by controller.", nil),
		gaugeSpec("controller_active_sessions", "Active sessions tracked by the controller.", nil),
		counterSpec("controller_unknown_ready_signal_total", "Ready signals for unknown sessions.", nil),
		// The lease allocators' observability. This registry only exports names
		// declared here — metric() returns nil and counts `unregistered_metric`
		// for anything else — so without these three specs the Gauge/Counter calls
		// in controller/lease.go and controller/consumer.go are silent no-ops and
		// the leases are unobservable from outside the controller's logs.
		// The autoscaling demand signal: sum of worker_count over in-flight
		// sessions, i.e. how many bot-fleet shards must be servable right now.
		// KEDA scales on this, so an unregistered name here means the fleet
		// never scales.
		gaugeSpec("controller_demanded_workers", "Bot-fleet workers demanded by in-flight sessions (sum of worker_count).", nil),
		gaugeSpec("controller_leased_partitions", "Resources (partitions) currently leased by in-flight sessions.", nil),
		gaugeSpec("controller_leased_order_bands", "Resources (order_bands) currently leased by in-flight sessions.", nil),
		counterSpec("controller_admission_blocked_total", "Session admissions blocked by scarce capacity.", []string{"reason"}),
		histogramSpec("db_query_duration_seconds", "PostgreSQL query duration in seconds.", []string{"operation", "service"}, defaultBuckets),
		counterSpec("db_query_total", "PostgreSQL queries by operation and result.", []string{"operation", "result", "service"}),
		counterSpec("harbor_promote_total", "Harbor image promotions by result.", []string{"result"}),
		histogramSpec("http_request_duration_seconds", "HTTP request duration in seconds.", []string{"method", "path", "service", "status"}, defaultBuckets),
		counterSpec("http_requests_total", "HTTP requests by service, method, path, and status.", []string{"method", "path", "service", "status"}),
		counterSpec("kafka_consumer_commit_total", "Kafka consumer commits by topic and result.", []string{"result", "service", "topic"}),
		histogramSpec("kafka_message_process_duration_seconds", "Kafka message processing duration in seconds.", []string{"result", "service", "topic"}, defaultBuckets),
		counterSpec("kafka_messages_consumed_total", "Kafka messages consumed by topic and result.", []string{"result", "service", "topic"}),
		counterSpec("kafka_messages_produced_total", "Kafka messages produced by topic and result.", []string{"result", "service", "topic"}),
		histogramSpec("kafka_produce_duration_seconds", "Kafka produce duration in seconds.", []string{"result", "service", "topic"}, defaultBuckets),
		histogramSpec("minio_operation_duration_seconds", "MinIO operation duration in seconds.", []string{"operation"}, defaultBuckets),
		gaugeSpec("pgxpool_acquired_conns", "Acquired pgxpool connections.", []string{"service"}),
		gaugeSpec("pgxpool_canceled_acquire_total", "pgxpool canceled acquire count.", []string{"service"}),
		gaugeSpec("pgxpool_empty_acquire_total", "pgxpool empty acquire count.", []string{"service"}),
		gaugeSpec("pgxpool_idle_conns", "Idle pgxpool connections.", []string{"service"}),
		gaugeSpec("pgxpool_max_conns", "Maximum pgxpool connections.", []string{"service"}),
		gaugeSpec("pgxpool_total_conns", "Total pgxpool connections.", []string{"service"}),
		counterSpec("ready_none_total", "Sessions with no ready signals before deadline.", nil),
		counterSpec("ready_partial_total", "Sessions with partial ready fan-in.", nil),
		counterSpec("ready_signals_total", "Ready fan-in completions by result.", []string{"result"}),
		counterSpec("recovery_inflight_runs_total", "In-flight runs recovered on controller startup.", nil),
		counterSpec("leaderboard_api_cache_reads_total", "Leaderboard API cache reads by result.", []string{"result"}),
		counterSpec("leaderboard_api_cache_writes_total", "Leaderboard API cache writes by result.", []string{"result"}),
		counterSpec("leaderboard_api_events_consumed_total", "Leaderboard update events consumed by result.", []string{"result"}),
		gaugeSpec("leaderboard_api_sse_clients", "Connected leaderboard SSE clients.", nil),
		counterSpec("leaderboard_api_sse_dropped_clients_total", "Leaderboard SSE clients dropped because they could not keep up.", nil),
		counterSpec("run_group_children_created_total", "Child runs created by scenario.", []string{"scenario_name"}),
		counterSpec("run_groups_created_total", "Run-groups created by submission-api.", nil),
		counterSpec("scorer_progress_events_total", "Scorer progress events persisted by source and result.", []string{"result", "source"}),
		counterSpec("scorer_run_groups_scored_total", "Run-groups scored by result.", []string{"result"}),
		histogramSpec("scorer_run_group_score_duration_seconds", "Score-computer run-group scoring duration in seconds.", []string{"result"}, defaultBuckets),
		counterSpec("run_status_updates_total", "Run status updates applied by submission-api.", []string{"status"}),
		histogramSpec("session_duration_seconds", "Benchmark session duration in seconds.", []string{"result", "scenario_name"}, defaultBuckets),
		histogramSpec("session_stage_duration_seconds", "Benchmark session stage duration in seconds.", []string{"result", "stage"}, defaultBuckets),
		counterSpec("session_stage_total", "Benchmark session stages by result.", []string{"result", "stage"}),
		counterSpec("session_transitions_total", "Session transitions published by status.", []string{"status"}),
		counterSpec("sessions_completed_total", "Benchmark sessions completed by scenario and result.", []string{"result", "scenario_name"}),
		counterSpec("sessions_started_total", "Benchmark sessions started by scenario.", []string{"scenario_name"}),
		histogramSpec("slot_create_duration_seconds", "Sandbox slot create duration in seconds.", []string{"result"}, defaultBuckets),
		counterSpec("slot_refresh_total", "Sandbox slot refreshes by state and result.", []string{"result", "state"}),
		counterSpec("slots_operations_total", "Sandbox slot operations by operation and result.", []string{"operation", "result"}),
		counterSpec("submission_duplicate_total", "Duplicate submissions detected by sha256.", nil),
		counterSpec("submission_status_transition_total", "Submission status transitions by status/result.", []string{"result", "status"}),
		histogramSpec("submission_upload_bytes", "Submission upload size in bytes.", nil, byteBuckets),
		counterSpec("submission_validation_failures_total", "Submission validation failures.", []string{"reason"}),
		counterSpec("submissions_accepted_total", "Accepted submissions by language and protocol.", []string{"language", "protocol"}),
		counterSpec("validator_sessions_validated_total", "Correctness-validator sessions processed by result.", []string{"result"}),
		counterSpec("validator_validation_errors_total", "Correctness-validator validation errors by stage.", []string{"stage"}),
		counterSpec("validator_violations_total", "Correctness violations detected by type.", []string{"type"}),
		counterSpec("validator_scores_published_total", "Correctness scores published to scores.correctness.", nil),
		histogramSpec("validator_drain_duration_seconds", "Correctness-validator per-session Kafka drain duration in seconds.", nil, defaultBuckets),
		counterSpec("validator_events_drained_total", "Correctness-validator events drained from Kafka by topic.", []string{"topic"}),
		gaugeSpec("validator_inflight_sessions", "Correctness-validator sessions currently being validated.", nil),
		histogramSpec("validator_session_events_buffered", "Events (orders.sent + orders.acked) buffered in memory per validated session.", nil, byteBuckets),
	}
}

// counterSpec performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func counterSpec(name, help string, labelKeys []string) metricSpec {
	return metricSpec{name: name, help: help, kind: kindCounter, labelKeys: labelKeys}
}

// gaugeSpec performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func gaugeSpec(name, help string, labelKeys []string) metricSpec {
	return metricSpec{name: name, help: help, kind: kindGauge, labelKeys: labelKeys}
}

// histogramSpec performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func histogramSpec(name, help string, labelKeys []string, buckets []float64) metricSpec {
	return metricSpec{name: name, help: help, kind: kindHistogram, labelKeys: labelKeys, buckets: buckets}
}

// bucketsForMetric performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func bucketsForMetric(name string) []float64 {
	name = normalizeName(name)
	for _, spec := range projectMetricCatalog() {
		if normalizeName(spec.name) == name && spec.kind == kindHistogram {
			return spec.buckets
		}
	}
	return defaultBuckets
}

// register applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *registry) register(c prometheus.Collector) bool {
	if err := r.prom.Register(c); err != nil {
		r.recordErrorLocked("collector_registration_failed")
		return false
	}
	return true
}

// normalizeName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func normalizeName(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, namespace+"_") {
		return sanitizeMetricName(name)
	}
	return sanitizeMetricName(namespace + "_" + name)
}

// normalizeBuckets performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func normalizeBuckets(buckets []float64) []float64 {
	cp := append([]float64(nil), buckets...)
	sort.Float64s(cp)
	out := cp[:0]
	last := math.NaN()
	for _, v := range cp {
		if v <= 0 || v == last {
			continue
		}
		out = append(out, v)
		last = v
	}
	return out
}

// sanitizedLabels performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sanitizedLabels(labels map[string]string, histogram bool) (map[string]string, bool) {
	if len(labels) == 0 {
		return nil, true
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		name := sanitizeLabelName(k)
		if histogram && name == "le" {
			name = "label_le"
		}
		if _, exists := out[name]; exists {
			return nil, false
		}
		out[name] = v
	}
	return out, true
}

// sanitizeCatalogLabelKeys performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sanitizeCatalogLabelKeys(keys []string, histogram bool) []string {
	if len(keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		name := sanitizeLabelName(key)
		if histogram && name == "le" {
			name = "label_le"
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// splitLabels performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func splitLabels(labels map[string]string) ([]string, []string) {
	if len(labels) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, orderedValues(labels, keys)
}

// orderedValues performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func orderedValues(labels map[string]string, keys []string) []string {
	values := make([]string, 0, len(keys))
	for _, k := range keys {
		values = append(values, labels[k])
	}
	return values
}

// sanitizeLabelName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sanitizeLabelName(name string) string {
	var b strings.Builder
	for i, r := range name {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || (i > 0 && r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

// sanitizeMetricName performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sanitizeMetricName(name string) string {
	var b strings.Builder
	for i, r := range name {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || (i > 0 && r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return namespace + "_invalid_metric"
	}
	return b.String()
}

// sameBuckets performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sameBuckets(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sameStrings performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// isFinite performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// recordError applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *registry) recordError(reason string) {
	r.errors.WithLabelValues(reason).Inc()
}

// recordErrorLocked applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (r *registry) recordErrorLocked(reason string) {
	r.errors.WithLabelValues(reason).Inc()
}

// RegistryErrorCount returns how many registry errors of `reason` have been
// recorded. Exported so a service's own tests can assert that the metrics it emits
// are actually exported.
//
// This registry deliberately exports only names present in its pre-declared catalog:
// emitting an undeclared name is a silent no-op that increments
// `unregistered_metric` and nothing else. That is easy to miss — three controller
// lease metrics shipped in exactly that state — so a test asserting this counter
// does not move is the cheapest guard against a metric that exists in code but never
// reaches Prometheus.
func RegistryErrorCount(reason string) uint64 {
	var m dto.Metric
	c, err := global.errors.GetMetricWithLabelValues(reason)
	if err != nil {
		return 0
	}
	if err := c.Write(&m); err != nil {
		return 0
	}
	return uint64(m.GetCounter().GetValue())
}
