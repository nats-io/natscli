package jsch_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nats-io/jetstream/jsch"
)

func TestNewStreamFromTemplate(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStreamFromTemplate("q1", jsch.DefaultWorkQueue, jsch.MemoryStorage(), jsch.MaxAge(time.Hour))
	checkErr(t, err, "create failed")

	stream.Reset()
	if stream.Name() != "q1" {
		t.Fatalf("incorrect name %q", stream.Name())
	}

	if stream.Storage() != server.MemoryStorage {
		t.Fatalf("incorrect storage, expected memory got %s", stream.Storage().String())
	}

	if stream.MaxAge() != time.Hour {
		t.Fatalf("incorrect max age, expected 1 hour got %v", stream.MaxAge())
	}
}

func TestLoadOrNewStreamFromTemplate(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	names, err := jsch.StreamNames()
	checkErr(t, err, "ls failed")
	if len(names) != 0 {
		t.Fatalf("expected no streams")
	}

	stream, err := jsch.LoadOrNewStreamFromTemplate("q1", jsch.DefaultWorkQueue, jsch.MemoryStorage(), jsch.MaxAge(time.Hour))
	checkErr(t, err, "create failed")

	_, err = jsch.LoadOrNewStreamFromTemplate("q1", stream.Configuration())
	checkErr(t, err, "load failed")

	names, err = jsch.StreamNames()
	checkErr(t, err, "ls failed")

	if len(names) != 1 || names[0] != "q1" {
		t.Fatalf("expected [q1] got %v", names)
	}
}

func TestNewStream(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	if stream.Name() != "q1" {
		t.Fatalf("expected q1 got %s", stream.Name())
	}

	if stream.Storage() != server.FileStorage {
		t.Fatalf("expected file storage got %s", stream.Storage().String())
	}

	if stream.Retention() != server.LimitsPolicy {
		t.Fatalf("expected stream retention got %s", stream.Retention().String())
	}
}

func TestLoadOrNewStream(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	names, err := jsch.StreamNames()
	checkErr(t, err, "ls failed")
	if len(names) != 0 {
		t.Fatalf("expected no streams")
	}

	stream, err := jsch.LoadOrNewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	if stream.Name() != "q1" {
		t.Fatalf("expected q1 got %s", stream.Name())
	}

	if stream.Storage() != server.FileStorage {
		t.Fatalf("expected file storage got %s", stream.Storage().String())
	}

	if stream.Retention() != server.LimitsPolicy {
		t.Fatalf("expected stream retention got %s", stream.Retention().String())
	}

	stream, err = jsch.LoadOrNewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	if stream.Name() != "q1" {
		t.Fatalf("expected q1 got %s", stream.Name())
	}
}

func TestLoadStream(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	names, err := jsch.StreamNames()
	checkErr(t, err, "ls failed")
	if len(names) != 0 {
		t.Fatalf("expected no streams")
	}

	_, err = jsch.LoadStream("q1")
	if err == nil {
		t.Fatal("expected error")
	}

	_, err = jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	stream, err := jsch.LoadStream("q1")
	checkErr(t, err, "load failed")

	if stream.Name() != "q1" {
		t.Fatalf("expected q1 got %s", stream.Name())
	}

	if stream.Storage() != server.FileStorage {
		t.Fatalf("expected file storage got %s", stream.Storage().String())
	}
}

func TestStream_Reset(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	orig, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	// make original inconsistent
	err = orig.Delete()
	checkErr(t, err, "delete failed")

	// create a new one in its place
	_, err = jsch.NewStream("q1", jsch.MemoryStorage())
	checkErr(t, err, "create failed")

	// reload
	err = orig.Reset()
	checkErr(t, err, "reset failed")

	// verify its good
	if orig.Storage() != server.MemoryStorage {
		t.Fatalf("expected memory storage got %s", orig.Storage().String())
	}
}

func TestStream_LoadConsumer(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	_, err = stream.NewConsumer(jsch.DurableName("c1"))
	checkErr(t, err, "create failed")

	consumer, err := stream.LoadConsumer("c1")
	checkErr(t, err, "load failed")

	if consumer.Name() != "c1" {
		t.Fatalf("expected consumer c1 got %s", consumer.Name())
	}
}

func TestStream_NewConsumerFromTemplate(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	consumer, err := stream.NewConsumerFromTemplate("q1", jsch.SampledDefaultConsumer, jsch.DurableName("q1"))
	checkErr(t, err, "create failed")
	consumer.Reset()
	if !consumer.IsSampled() {
		t.Fatalf("expected a sampled consumer")
	}
}

func TestStream_LoadOrNewConsumerFromTemplate(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	_, err = stream.LoadOrNewConsumerFromTemplate("q1", jsch.SampledDefaultConsumer, jsch.DurableName("q1"))
	checkErr(t, err, "create failed")

	consumer, err := stream.LoadOrNewConsumerFromTemplate("q1", jsch.SampledDefaultConsumer, jsch.DurableName("q1"))
	checkErr(t, err, "load failed")
	if !consumer.IsSampled() {
		t.Fatalf("expected a sampled consumer")
	}
}

func TestStream_ConsumerNames(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	_, err = stream.NewConsumer(jsch.DurableName("c1"))
	checkErr(t, err, "create failed")

	_, err = stream.NewConsumer(jsch.DurableName("c2"))
	checkErr(t, err, "create failed")

	names, err := stream.ConsumerNames()
	checkErr(t, err, "names failed")

	if len(names) != 2 || names[0] != "c1" || names[1] != "c2" {
		t.Fatalf("expected [c1, c2] got %v", names)
	}
}

func TestStream_Information(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage())
	checkErr(t, err, "create failed")

	info, err := stream.Information()
	checkErr(t, err, "info failed")

	if info.Config.Name != "q1" {
		t.Fatalf("expected q1 got %s", info.Config.Name)
	}
}

func TestStream_Statistics(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage(), jsch.Subjects("test"))
	checkErr(t, err, "create failed")

	stats, err := stream.State()
	checkErr(t, err, "stats failed")
	if stats.Msgs != 0 {
		t.Fatalf("expected 0 messages got %d", stats.Msgs)
	}

	nc.Publish(stream.Subjects()[0], []byte("message 1"))

	stats, err = stream.State()
	checkErr(t, err, "stats failed")
	if stats.Msgs != 1 {
		t.Fatalf("expected 1 messages got %d", stats.Msgs)
	}
}

func TestStream_Delete(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage(), jsch.Subjects("test"))
	checkErr(t, err, "create failed")

	err = stream.Delete()
	checkErr(t, err, "delete failed")

	names, err := jsch.StreamNames()
	checkErr(t, err, "names failed")

	if len(names) != 0 {
		t.Fatalf("got streams where none should be: %v", names)
	}
}

func TestStream_Purge(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage(), jsch.Subjects("test"))
	checkErr(t, err, "create failed")

	nc.Publish(stream.Subjects()[0], []byte("message 1"))

	stats, err := stream.State()
	checkErr(t, err, "stats failed")
	if stats.Msgs != 1 {
		t.Fatalf("expected 1 messages got %d", stats.Msgs)
	}

	err = stream.Purge()
	checkErr(t, err, "purge failed")

	stats, err = stream.State()
	checkErr(t, err, "stats failed")
	if stats.Msgs != 0 {
		t.Fatalf("expected 0 messages got %d", stats.Msgs)
	}
}

func TestStream_LoadMessage(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage(), jsch.Subjects("test"))
	checkErr(t, err, "create failed")

	nc.Publish(stream.Subjects()[0], []byte("message 1"))

	msg, err := stream.LoadMessage(1)
	checkErr(t, err, "load failed")
	if string(msg.Data) != "message 1" {
		t.Fatalf("expected message 1 got %q", msg.Data)
	}

	msg, err = stream.LoadMessage(2)
	if err == nil {
		t.Fatalf("LoadMessage didnt fail on unknown message")
	}

}

func TestStream_DeleteMessage(t *testing.T) {
	srv, nc := startJSServer(t)
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := jsch.NewStream("q1", jsch.FileStorage(), jsch.Subjects("test"))
	checkErr(t, err, "create failed")

	nc.Publish(stream.Subjects()[0], []byte("message 1"))

	_, err = stream.LoadMessage(1)
	checkErr(t, err, "load failed")

	err = stream.DeleteMessage(1)
	checkErr(t, err, "delete failed")

	msg, err := stream.LoadMessage(1)
	if err == nil {
		t.Fatalf("deleted messages was loaded %q", msg.Data)
	}
}

func TestFileStorage(t *testing.T) {
	cfg := server.StreamConfig{Storage: -1}
	err := jsch.FileStorage()(&cfg)
	checkErr(t, err, "failed")
	if cfg.Storage != server.FileStorage {
		t.Fatalf("expected FileStorage")
	}
}

func TestInterestRetention(t *testing.T) {
	cfg := server.StreamConfig{Retention: -1}
	err := jsch.InterestRetention()(&cfg)
	checkErr(t, err, "failed")
	if cfg.Retention != server.InterestPolicy {
		t.Fatalf("expected InterestPolicy")
	}
}

func TestMaxAge(t *testing.T) {
	cfg := server.StreamConfig{MaxAge: -1}
	err := jsch.MaxAge(time.Hour)(&cfg)
	checkErr(t, err, "failed")
	if cfg.MaxAge != time.Hour {
		t.Fatalf("expected 1 hour")
	}
}

func TestMaxBytes(t *testing.T) {
	cfg := server.StreamConfig{MaxBytes: -1}
	err := jsch.MaxBytes(1024)(&cfg)
	checkErr(t, err, "failed")
	if cfg.MaxBytes != 1024 {
		t.Fatalf("expected 1024")
	}
}

func TestMaxMessageSize(t *testing.T) {
	cfg := server.StreamConfig{MaxMsgSize: -1}
	err := jsch.MaxMessageSize(1024)(&cfg)
	checkErr(t, err, "failed")
	if cfg.MaxMsgSize != 1024 {
		t.Fatalf("expected 1024")
	}
}

func TestMaxMessages(t *testing.T) {
	cfg := server.StreamConfig{MaxMsgs: -1}
	err := jsch.MaxMessages(1024)(&cfg)
	checkErr(t, err, "failed")
	if cfg.MaxMsgs != 1024 {
		t.Fatalf("expected 1024")
	}
}

func TestMaxObservables(t *testing.T) {
	cfg := server.StreamConfig{MaxConsumers: -1}
	err := jsch.MaxConsumers(1024)(&cfg)
	checkErr(t, err, "failed")
	if cfg.MaxConsumers != 1024 {
		t.Fatalf("expected 1024")
	}
}

func TestMemoryStorage(t *testing.T) {
	cfg := server.StreamConfig{Storage: -1}
	err := jsch.MemoryStorage()(&cfg)
	checkErr(t, err, "memory storage failed")
	if cfg.Storage != server.MemoryStorage {
		t.Fatalf("expected MemoryStorage")
	}
}

func TestNoAck(t *testing.T) {
	cfg := server.StreamConfig{NoAck: false}
	err := jsch.NoAck()(&cfg)
	checkErr(t, err, "failed")
	if !cfg.NoAck {
		t.Fatalf("expected NoAck")
	}
}

func TestReplicas(t *testing.T) {
	cfg := server.StreamConfig{Replicas: -1}
	err := jsch.Replicas(1024)(&cfg)
	checkErr(t, err, "failed")
	if cfg.Replicas != 1024 {
		t.Fatalf("expected 1024")
	}
}

func TestLimitsRetention(t *testing.T) {
	cfg := server.StreamConfig{Retention: -1}
	err := jsch.LimitsRetention()(&cfg)
	checkErr(t, err, "failed")
	if cfg.Retention != server.LimitsPolicy {
		t.Fatalf("expected LimitsPolicy")
	}
}

func TestSubjects(t *testing.T) {
	cfg := server.StreamConfig{Subjects: []string{}}
	err := jsch.Subjects("one", "two")(&cfg)
	checkErr(t, err, "failed")
	if len(cfg.Subjects) != 2 || cfg.Subjects[0] != "one" || cfg.Subjects[1] != "two" {
		t.Fatalf("expected [one, two] got %v", cfg.Subjects)
	}
}

func TestWorkQueueRetention(t *testing.T) {
	cfg := server.StreamConfig{Retention: -1}
	err := jsch.WorkQueueRetention()(&cfg)
	checkErr(t, err, "failed")
	if cfg.Retention != server.WorkQueuePolicy {
		t.Fatalf("expected WorkQueuePolicy")
	}
}
