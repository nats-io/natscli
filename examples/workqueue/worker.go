package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
)

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

func panicIfError(err error) {
	if err == nil {
		return
	}

	panic(err)
}

func convertMessage(job *Job, outDir string) error {
	uri, err := url.Parse(job.URI)
	panicIfError(err)

	// timeout the command after 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	outfile := filepath.Join(outDir, fmt.Sprintf("%s%s", job.ID, filepath.Ext(uri.Path)))
	log.Printf("Converting %s into %s", uri.Path, outfile)

	// use ImageMagick to convert the image
	convert := exec.CommandContext(ctx, "/bin/convert", "-monochrome", uri.Path, outfile)
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

func processNextMessage(nc *nats.Conn, outDir string) error {
	log.Printf("Looking for a B&W conversion job")

	// Load the next message from the IMAGES stream BW consumer
	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.IMAGES.BW", []byte(""), time.Minute)
	// ErrTimeout means there is no new message now lets just skip
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

func main() {
	nc, err := nats.Connect(os.Getenv("NATS_URL"), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		panic(err)
	}))
	panicIfError(err)

	outDir := os.Getenv("OUTDIR")
	if outDir == "" {
		panic("Please set OUTDIR")
	}

	for {
		err = processNextMessage(nc, outDir)
		panicIfError(err)
	}
}
