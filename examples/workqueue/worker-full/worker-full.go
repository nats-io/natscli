package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/nats-io/jsm.go"
	natsd "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func main() {
	natsURL, err := url.Parse(os.Getenv("NATS_URL"))
	if err != nil {
		panic(err)
	}
	outDir := os.Getenv("OUTDIR")

	srv, nc, err := StartJSServer(natsURL)
	if err != nil {
		panic(err)
	}
	defer srv.Shutdown()
	defer nc.Flush()

	stream, err := AddStream("IMAGES", nc)
	if err != nil {
		panic(err)
	}
	consumer, err := AddConsumer("BW", "IMAGES.blackandwhite", stream.Name(), nc)
	if err != nil {
		panic(err)
	}

	if err = sendMessage(consumer.FilterSubject(), nc); err != nil {
		panic(err)
	}

	if err = processNextMessage(stream.Name(), consumer.DurableName(), outDir, nc); err != nil {
		panic(err)
	}
}

func StartJSServer(url *url.URL) (*natsd.Server, *nats.Conn, error) {
	dir, err := ioutil.TempDir("", "nats-jetstream-*")
	if err != nil {
		return nil, nil, err
	}

	natsPort, err := strconv.Atoi(url.Port())
	if err != nil {
		return nil, nil, err
	}

	opts := &natsd.Options{
		JetStream: true,
		StoreDir:  dir,
		Host:      url.Hostname(),
		Port:      natsPort,
		LogFile:   "/dev/stdout",
		Trace:     true,
	}
	s, err := natsd.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		return nil, nil, errors.New("nats server didn't start")
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		return nil, nil, err
	}

	return s, nc, nil
}

func AddStream(name string, nc *nats.Conn) (*jsm.Stream, error) {
	stream, err := jsm.NewStreamFromDefault(name, jsm.DefaultWorkQueue, jsm.StreamConnection(
		jsm.WithConnection(nc)), jsm.Subjects(name+".*"), jsm.FileStorage(), jsm.MaxAge(24*365*time.Hour),
		jsm.DiscardOld(), jsm.MaxMessages(-1), jsm.MaxBytes(-1), jsm.MaxMessageSize(512),
		jsm.DuplicateWindow(1*time.Hour))
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func AddConsumer(name, filter, stream string, nc *nats.Conn) (*jsm.Consumer, error) {
	consumer, err := jsm.NewConsumerFromDefault(stream, jsm.DefaultConsumer, jsm.ConsumerConnection(
		jsm.WithConnection(nc)), jsm.DurableName(name), jsm.MaxDeliveryAttempts(5),
		jsm.AckWait(30*time.Second), jsm.AcknowledgeExplicit(), jsm.ReplayInstantly(),
		jsm.DeliverAllAvailable(), jsm.FilterStreamBySubject(filter))
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// Job is the request to convert an image
type Job struct {
	ID       string `json:"id"`
	URI      string `json:"image"`
	Advisory string `json:"advisory"`
}

// Advisory is an advisory published to notify others that an image is done
type Advisory struct {
	ID string `json:"id"`
}

func sendMessage(subj string, nc *nats.Conn) error {
	job := Job{
		ID:       "123456",
		URI:      "/tmp/images/input/color.jpg",
		Advisory: "",
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err = nc.Publish(subj, data); err != nil {
		return err
	}

	return nil
}

func processNextMessage(stream, consumer, outDir string, nc *nats.Conn) error {
	msg, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.%s.%s", stream, consumer), []byte(""), time.Minute)
	if err == nats.ErrTimeout {
		return nil
	}
	if err != nil {
		return err
	}
	// Parse the job
	job := &Job{}
	err = json.Unmarshal(msg.Data, job)
	if err != nil {
		return err
	}

	log.Printf("Processing %#v", job)

	// Process the image into B&W
	err = convertMessage(job, outDir)
	if err != nil {
		return err
	}

	// Advise that it was completed
	if job.Advisory != "" {
		err = publishAdvisory(job.ID, job.Advisory, nc)
		if err != nil {
			return err
		}
	}

	// Acknowledge the message in JetStream, this will delete it from the work queue
	return msg.Respond(nil)
}

func convertMessage(job *Job, outDir string) error {
	uri, err := url.Parse(job.URI)
	if err != nil {
		return err
	}
	// timeout the command after 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	outfile := filepath.Join(outDir, fmt.Sprintf("%s%s", job.ID, filepath.Ext(uri.Path)))
	log.Printf("Converting %s into %s", uri.Path, outfile)

	// use ImageMagick to convert the image

	convert := exec.CommandContext(ctx, "/usr/bin/convert", "-monochrome", uri.Path, outfile)
	return convert.Run()
}

func publishAdvisory(id string, target string, nc *nats.Conn) error {
	// Publish an advisory to IMAGES.advisory
	advisory := &Advisory{id}
	advj, err := json.Marshal(advisory)
	if err != nil {
		return err
	}

	return nc.Publish(target, advj)
}
