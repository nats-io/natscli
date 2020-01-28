package jsch_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jetstream/jsch"
)

func setupConsumerTest(t *testing.T) (*server.Server, *nats.Conn, *jsch.Stream) {
	t.Helper()
	srv, nc := startJSServer(t)
	stream, err := jsch.NewStreamFromDefault("ORDERS", jsch.DefaultStream, jsch.MemoryStorage(), jsch.MaxAge(time.Hour), jsch.Subjects("ORDERS.>"))
	checkErr(t, err, "create failed")

	_, err = nc.Request("ORDERS.new", []byte("order 1"), time.Second)
	checkErr(t, err, "publish failed")

	return srv, nc, stream

}

func TestNewConsumer(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	consumer, err := jsch.NewConsumer("ORDERS", jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	consumer.Reset()
	if consumer.AckPolicy() != server.AckExplicit {
		t.Fatalf("expected explicit ack got %s", consumer.AckPolicy().String())
	}

	if consumer.Name() != "NEW" {
		t.Fatalf("expected NEW got %s", consumer.Name())
	}
}

func TestNewConsumerFromTemplateDurable(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	consumer, err := jsch.NewConsumerFromDefault("ORDERS", jsch.SampledDefaultConsumer, jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	consumer.Reset()
	if consumer.AckPolicy() != server.AckExplicit {
		t.Fatalf("expected explicit ack got %s", consumer.AckPolicy().String())
	}

	if consumer.Name() != "NEW" {
		t.Fatalf("expected NEW got %s", consumer.Name())
	}

	if !consumer.IsSampled() {
		t.Fatal("expected a sampled consumer")
	}
}

func TestNewConsumerFromTemplateEphemeral(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	// interest is needed
	nc.Subscribe("out", func(_ *nats.Msg) {})

	consumer, err := jsch.NewConsumerFromDefault("ORDERS", jsch.SampledDefaultConsumer, jsch.DeliverySubject("out"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	consumers, err := jsch.ConsumerNames("ORDERS")
	checkErr(t, err, "consumer list failed")
	if len(consumers) != 1 {
		t.Fatalf("expected 1 consumer got %v", consumers)
	}

	if consumer.Name() != consumers[0] {
		t.Fatalf("incorrect consumer name '%s' expected '%s'", consumer.Name(), consumers[0])
	}

	if consumer.IsDurable() {
		t.Fatalf("expected ephemeral consumer got durable")
	}
}

func TestLoadConsumer(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	_, err := jsch.NewConsumerFromDefault("ORDERS", jsch.SampledDefaultConsumer, jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	consumer, err := jsch.LoadConsumer("ORDERS", "NEW")
	checkErr(t, err, "load failed")

	if consumer.AckPolicy() != server.AckExplicit {
		t.Fatalf("expected explicit ack got %s", consumer.AckPolicy().String())
	}

	if consumer.Name() != "NEW" {
		t.Fatalf("expected NEW got %s", consumer.Name())
	}

	if !consumer.IsSampled() {
		t.Fatal("expected a sampled consumer")
	}
}

func TestLoadOrNewConsumer(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	_, err := jsch.LoadOrNewConsumer("ORDERS", "NEW", jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	consumer, err := jsch.LoadOrNewConsumer("ORDERS", "NEW", jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "load failed")

	if consumer.AckPolicy() != server.AckExplicit {
		t.Fatalf("expected explicit ack got %s", consumer.AckPolicy().String())
	}

	if consumer.Name() != "NEW" {
		t.Fatalf("expected NEW got %s", consumer.Name())
	}
}

func TestLoadOrNewConsumerFromTemplate(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	_, err := jsch.LoadOrNewConsumerFromDefault("ORDERS", "NEW", jsch.SampledDefaultConsumer, jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	consumer, err := jsch.LoadOrNewConsumerFromDefault("ORDERS", "NEW", jsch.SampledDefaultConsumer, jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "load failed")

	if consumer.AckPolicy() != server.AckExplicit {
		t.Fatalf("expected explicit ack got %s", consumer.AckPolicy().String())
	}

	if consumer.Name() != "NEW" {
		t.Fatalf("expected NEW got %s", consumer.Name())
	}

	if !consumer.IsSampled() {
		t.Fatal("expected a sampled consumer")
	}
}

func TestConsumer_Reset(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	consumer, err := jsch.NewConsumer("ORDERS", jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	err = consumer.Delete()
	checkErr(t, err, "delete failed")

	consumer, err = jsch.NewConsumer("ORDERS", jsch.DurableName("NEW"))
	checkErr(t, err, "create failed")

	err = consumer.Reset()
	checkErr(t, err, "reset failed")

	if consumer.FilterSubject() != "" {
		t.Fatalf("expected no filter got %v", consumer.FilterSubject())
	}
}

func TestConsumer_NextSubject(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	consumer, err := jsch.NewConsumer("ORDERS", jsch.DurableName("NEW"), jsch.FilterStreamBySubject("ORDERS.new"))
	checkErr(t, err, "create failed")

	if consumer.NextSubject() != "$JS.STREAM.ORDERS.CONSUMER.NEW.NEXT" {
		t.Fatalf("expected next subject got %s", consumer.NextSubject())
	}
}

func TestConsumer_SampleSubject(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	consumer, err := jsch.NewConsumerFromDefault("ORDERS", jsch.SampledDefaultConsumer, jsch.DurableName("NEW"))
	checkErr(t, err, "create failed")

	if consumer.AckSampleSubject() != "$JS.EVENT.METRIC.CONSUMER_ACK.ORDERS.NEW" {
		t.Fatalf("expected next subject got %s", consumer.AckSampleSubject())
	}

	unsampled, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("UNSAMPLED"))
	checkErr(t, err, "create failed")

	if unsampled.AckSampleSubject() != "" {
		t.Fatalf("expected empty next subject got %s", consumer.AckSampleSubject())
	}
}

func TestConsumer_State(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	durable, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("D"))
	checkErr(t, err, "create failed")

	state, err := durable.State()
	checkErr(t, err, "state failed")

	if state.Delivered.StreamSeq != 0 {
		t.Fatalf("expected set seq 0 got %d", state.Delivered.StreamSeq)
	}

	m, err := durable.NextMsg()
	checkErr(t, err, "next failed")
	err = m.Respond(nil)
	checkErr(t, err, "ack failed")

	state, err = durable.State()
	checkErr(t, err, "state failed")

	if state.Delivered.StreamSeq != 1 {
		t.Fatalf("expected set seq 1 got %d", state.Delivered.StreamSeq)
	}

}

func TestConsumer_Configuration(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	durable, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("D"))
	checkErr(t, err, "create failed")

	if durable.Configuration().Durable != "D" {
		t.Fatalf("got wrong config: %+v", durable.Configuration())
	}
}

func TestConsumer_Delete(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	durable, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("D"))
	checkErr(t, err, "create failed")
	if !durable.IsDurable() {
		t.Fatalf("expected durable, got %s", durable.DurableName())
	}

	err = durable.Delete()
	checkErr(t, err, "delete failed")

	names, err := jsch.ConsumerNames("ORDERS")
	checkErr(t, err, "names failed")

	if len(names) != 0 {
		t.Fatalf("expected [] got %v", names)
	}
}

func TestConsumer_IsDurable(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	durable, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("D"))
	checkErr(t, err, "create failed")
	if !durable.IsDurable() {
		t.Fatalf("expected durable, got %s", durable.DurableName())
	}
	durable.Delete()

	// interest is needed before creating a ephemeral push
	nc.Subscribe("out", func(_ *nats.Msg) {})

	_, err = jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DeliverySubject("out"))
	checkErr(t, err, "create failed")

	names, err := jsch.ConsumerNames("ORDERS")
	checkErr(t, err, "names failed")

	if len(names) == 0 {
		t.Fatal("got no consumers")
	}

	eph, err := jsch.LoadConsumer("ORDERS", names[0])
	checkErr(t, err, "load failed")
	if eph.IsDurable() {
		t.Fatalf("expected ephemeral got %q %q", eph.Name(), eph.DurableName())
	}
}

func TestConsumer_IsPullMode(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	push, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("PUSH"), jsch.DeliverySubject("out"))
	checkErr(t, err, "create failed")
	if push.IsPullMode() {
		t.Fatalf("expected push, got %s", push.DeliverySubject())
	}

	pull, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("PULL"))
	checkErr(t, err, "create failed")
	if !pull.IsPullMode() {
		t.Fatalf("expected pull, got %s", pull.DeliverySubject())
	}
}

func TestConsumer_IsPushMode(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	push, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("PUSH"), jsch.DeliverySubject("out"))
	checkErr(t, err, "create failed")
	if !push.IsPushMode() {
		t.Fatalf("expected push, got %s", push.DeliverySubject())
	}

	pull, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("PULL"))
	checkErr(t, err, "create failed")
	if pull.IsPushMode() {
		t.Fatalf("expected pull, got %s", pull.DeliverySubject())
	}
}

func TestConsumer_IsSampled(t *testing.T) {
	srv, nc, _ := setupConsumerTest(t)
	defer srv.Shutdown()
	defer nc.Flush()

	sampled, err := jsch.NewConsumerFromDefault("ORDERS", jsch.SampledDefaultConsumer, jsch.DurableName("SAMPLED"))
	checkErr(t, err, "create failed")
	if !sampled.IsSampled() {
		t.Fatalf("expected sampled, got %s", sampled.SampleFrequency())
	}

	unsampled, err := jsch.NewConsumerFromDefault("ORDERS", jsch.DefaultConsumer, jsch.DurableName("UNSAMPLED"))
	checkErr(t, err, "create failed")
	if unsampled.IsSampled() {
		t.Fatalf("expected un-sampled, got %s", unsampled.SampleFrequency())
	}
}

func TestAckWait(t *testing.T) {
	cfg := server.ConsumerConfig{AckWait: 0}
	jsch.AckWait(time.Hour)(&cfg)
	if cfg.AckWait != time.Hour {
		t.Fatalf("expected 1 hour got %v", cfg.AckWait)
	}
}

func TestAcknowledgeAll(t *testing.T) {
	cfg := server.ConsumerConfig{AckPolicy: -1}
	jsch.AcknowledgeAll()(&cfg)
	if cfg.AckPolicy != server.AckAll {
		t.Fatalf("expected AckAll got %s", cfg.AckPolicy.String())
	}
}

func TestAcknowledgeExplicit(t *testing.T) {
	cfg := server.ConsumerConfig{AckPolicy: -1}
	jsch.AcknowledgeExplicit()(&cfg)
	if cfg.AckPolicy != server.AckExplicit {
		t.Fatalf("expected AckExplicit got %s", cfg.AckPolicy.String())
	}
}

func TestAcknowledgeNone(t *testing.T) {
	cfg := server.ConsumerConfig{AckPolicy: -1}
	jsch.AcknowledgeNone()(&cfg)
	if cfg.AckPolicy != server.AckNone {
		t.Fatalf("expected AckNone got %s", cfg.AckPolicy.String())
	}
}

func TestDeliverAllAvailable(t *testing.T) {
	cfg := server.ConsumerConfig{DeliverAll: false}
	jsch.DeliverAllAvailable()(&cfg)
	if !cfg.DeliverAll {
		t.Fatal("expected DeliverAll")
	}
}

func TestDeliverySubject(t *testing.T) {
	cfg := server.ConsumerConfig{Delivery: ""}
	jsch.DeliverySubject("out")(&cfg)
	if cfg.Delivery != "out" {
		t.Fatalf("expected 'out' got %q", cfg.Delivery)
	}
}

func TestDurableName(t *testing.T) {
	cfg := server.ConsumerConfig{Durable: ""}
	jsch.DurableName("test")(&cfg)
	if cfg.Durable != "test" {
		t.Fatalf("expected 'test' got %q", cfg.Durable)
	}
}

func TestFilterStreamBySubject(t *testing.T) {
	cfg := server.ConsumerConfig{FilterSubject: ""}
	jsch.FilterStreamBySubject("test")(&cfg)
	if cfg.FilterSubject != "test" {
		t.Fatalf("expected 'test' got %q", cfg.FilterSubject)
	}
}

func TestMaxDeliveryAttempts(t *testing.T) {
	cfg := server.ConsumerConfig{MaxDeliver: -1}
	jsch.MaxDeliveryAttempts(10)(&cfg)
	if cfg.MaxDeliver != 10 {
		t.Fatalf("expected 10 got %q", cfg.MaxDeliver)
	}

	err := jsch.MaxDeliveryAttempts(0)
	if err == nil {
		t.Fatalf("expected 0 deliveries to fail")
	}
}

func TestReplayAsReceived(t *testing.T) {
	cfg := server.ConsumerConfig{ReplayPolicy: -1}
	jsch.ReplayAsReceived()(&cfg)
	if cfg.ReplayPolicy != server.ReplayOriginal {
		t.Fatalf("expected ReplayOriginal got %s", cfg.ReplayPolicy.String())
	}
}

func TestReplayInstantly(t *testing.T) {
	cfg := server.ConsumerConfig{ReplayPolicy: -1}
	jsch.ReplayInstantly()(&cfg)
	if cfg.ReplayPolicy != server.ReplayInstant {
		t.Fatalf("expected ReplayInstant got %s", cfg.ReplayPolicy.String())
	}
}

func TestSamplePercent(t *testing.T) {
	cfg := server.ConsumerConfig{SampleFrequency: ""}
	err := jsch.SamplePercent(200)(&cfg)
	if err == nil {
		t.Fatal("impossible percent didnt error")
	}

	err = jsch.SamplePercent(-1)(&cfg)
	if err == nil {
		t.Fatal("impossible percent didnt error")
	}

	err = jsch.SamplePercent(0)(&cfg)
	checkErr(t, err, "good percent errored")
	if cfg.SampleFrequency != "" {
		t.Fatal("expected empty string")
	}

	err = jsch.SamplePercent(20)(&cfg)
	checkErr(t, err, "good percent errored")
	if cfg.SampleFrequency != "20%" {
		t.Fatal("expected 20 pct string")
	}
}

func TestStartAtSequence(t *testing.T) {
	cfg := server.ConsumerConfig{StreamSeq: 0}
	jsch.StartAtSequence(1024)(&cfg)
	if cfg.StreamSeq != 1024 {
		t.Fatal("expected 1024")
	}
}

func TestStartAtTime(t *testing.T) {
	cfg := server.ConsumerConfig{StartTime: time.Now()}
	s := time.Now().Add(-1 * time.Hour)
	jsch.StartAtTime(s)(&cfg)
	if cfg.StartTime.Unix() != s.Unix() {
		t.Fatal("expected 1 hour delta")
	}
}

func TestStartAtTimeDelta(t *testing.T) {
	cfg := server.ConsumerConfig{StartTime: time.Now()}
	jsch.StartAtTimeDelta(time.Hour)(&cfg)
	if cfg.StartTime.Unix() < time.Now().Add(-1*time.Hour-time.Second).Unix() || cfg.StartTime.Unix() > time.Now().Add(-1*time.Hour+time.Second).Unix() {
		t.Fatal("expected ~ 1 hour delta")
	}
}

func TestStartWithLastReceived(t *testing.T) {
	cfg := server.ConsumerConfig{DeliverLast: false}
	jsch.StartWithLastReceived()(&cfg)
	if !cfg.DeliverLast {
		t.Fatal("expected DeliverLast")
	}
}
