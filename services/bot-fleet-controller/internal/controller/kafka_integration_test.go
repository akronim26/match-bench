// Package controller defines tests for kafka integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package controller

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/iicpc/schemas/topics"
	kafka "github.com/segmentio/kafka-go"
)

// TestProducerIntegrationPublishesControllerTopics performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestProducerIntegrationPublishesControllerTopics(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicWorkloadAssignments, 24)
	ensureTopic(t, brokers, topics.TopicBarrier, 3)
	ensureTopic(t, brokers, topics.TopicBenchmarkStatusUpdated, 3)

	producer := NewProducer(strings.Join(brokers, ","), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	t.Cleanup(func() { _ = producer.Close() })

	suffix := integrationSuffix()
	sessionID := "itest-controller-" + suffix
	spec := topics.WorkloadSpec{
		SessionID:        sessionID,
		SubmissionID:     "sub-" + suffix,
		ContestantID:     "contestant-" + suffix,
		TargetHost:       "algo-" + suffix + ".sandbox.svc.cluster.local",
		TargetPort:       9898,
		Protocol:         "FIX",
		WorkerIndex:      7,
		WorkerCount:      24,
		GlobalSeed:       42,
		FIXVersion:       "FIX.4.2",
		ConnectTimeoutMS: 1500,
		WriteTimeoutMS:   250,
		Tasks: []topics.TaskSpec{{
			TaskID:     1,
			Profile:    "hft",
			TargetRPS:  10,
			DurationNs: 1_000_000,
		}},
	}
	workloadKey := sessionID + ":7"
	if err := producer.PublishWorkloadSpec(context.Background(), []topics.WorkloadSpec{spec}, []int{7}); err != nil {
		t.Fatalf("publish workload: %v", err)
	}
	if err := producer.PublishBarrier(context.Background(), sessionID, 123456789); err != nil {
		t.Fatalf("publish barrier: %v", err)
	}
	statusEvent := topics.BenchmarkStatusUpdated{
		SessionID:    sessionID,
		SubmissionID: spec.SubmissionID,
		RunGroupID:   "group-" + suffix,
		Status:       topics.RunStatusRunning,
		Message:      "integration running",
		UpdatedAt:    time.Now().UTC(),
	}
	if err := producer.PublishStatus(context.Background(), statusEvent); err != nil {
		t.Fatalf("publish status: %v", err)
	}

	var gotSpec topics.WorkloadSpec
	specPartition := consumeJSONByKey(t, brokers, topics.TopicWorkloadAssignments, workloadKey, &gotSpec)
	if gotSpec.SessionID != sessionID || gotSpec.WorkerIndex != 7 || len(gotSpec.Tasks) != 1 {
		t.Fatalf("unexpected workload spec: %+v", gotSpec)
	}
	if specPartition != 7 {
		t.Fatalf("workload spec for worker_index 7 landed on partition %d, want 7", specPartition)
	}

	var barrier topics.BarrierEvent
	consumeJSONByKey(t, brokers, topics.TopicBarrier, sessionID, &barrier)
	if barrier.SessionID != sessionID || barrier.TargetEpochUnixNanos != 123456789 {
		t.Fatalf("unexpected barrier: %+v", barrier)
	}

	var status topics.BenchmarkStatusUpdated
	consumeJSONByKey(t, brokers, topics.TopicBenchmarkStatusUpdated, sessionID, &status)
	if status.SessionID != sessionID || status.Status != topics.RunStatusRunning {
		t.Fatalf("unexpected status: %+v", status)
	}
}

// TestConsumerIntegrationDispatchesBotReady performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestConsumerIntegrationDispatchesBotReady(t *testing.T) {
	brokers := integrationBrokers(t)
	ensureTopic(t, brokers, topics.TopicBotReady, 3)

	suffix := integrationSuffix()
	sessionID := "itest-controller-ready-" + suffix
	sessions := NewSessionManager()
	sess := newTestSession(sessionID)
	sessions.Add(sess)

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(
		strings.Join(brokers, ","),
		"itest-controller-benchmark-unused-"+suffix,
		"itest-controller-ready-"+suffix,
		nil,
		nil,
		sessions,
		slog.New(slog.NewTextHandler(os.Stderr, nil)),
	)
	t.Cleanup(func() {
		cancel()
		consumer.Close()
	})
	go consumer.StartBotReady(ctx)

	sig := topics.ReadySignal{
		SessionID:   sessionID,
		WorkerID:    "worker-" + suffix,
		WorkerIndex: 2,
		WorkerCount: 4,
	}
	publishJSON(t, brokers, topics.TopicBotReady, sessionID+":2", sig)

	select {
	case got := <-sess.readyCh:
		if got.SessionID != sig.SessionID || got.WorkerID != sig.WorkerID || got.WorkerIndex != sig.WorkerIndex {
			t.Fatalf("unexpected ready signal: %+v", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for bot.ready dispatch from Kafka")
	}
}

// integrationBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func integrationBrokers(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("KAFKA_BROKERS")
	if strings.TrimSpace(raw) == "" {
		t.Skip("KAFKA_BROKERS is not set; skipping real Kafka integration test")
	}
	var brokers []string
	for _, part := range strings.Split(raw, ",") {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("KAFKA_BROKERS contains no usable brokers")
	}
	return brokers
}

// integrationSuffix performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func integrationSuffix() string {
	return strings.ReplaceAll(time.Now().UTC().Format("20060102150405.000000000"), ".", "")
}

// ensureTopic performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ensureTopic(t *testing.T, brokers []string, topic string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka admin: %v", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 3,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "min.insync.replicas", ConfigValue: "2"},
			{ConfigName: "max.message.bytes", ConfigValue: "1048576"},
		},
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		t.Fatalf("create topic %s: %v", topic, err)
	}
}

// publishJSON performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func publishJSON[T any](t *testing.T, brokers []string, topic, key string, value T) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %s key %s: %v", topic, key, err)
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		t.Fatalf("publish %s key %s: %v", topic, key, err)
	}
}

// consumeJSONByKey performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func consumeJSONByKey[T any](t *testing.T, brokers []string, topic, key string, dst *T) int {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        "itest-" + topic + "-" + integrationSuffix(),
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 0,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("fetch %s key %s: %v", topic, key, err)
		}
		if string(msg.Key) == key {
			if err := json.Unmarshal(msg.Value, dst); err != nil {
				t.Fatalf("decode %s key %s: %v", topic, key, err)
			}
			_ = reader.CommitMessages(context.Background(), msg)
			return msg.Partition
		}
		_ = reader.CommitMessages(context.Background(), msg)
	}
}
