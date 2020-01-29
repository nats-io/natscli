package jsch

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

type BackupData struct {
	Type          string `json:"type"`
	Time          string `json:"time"`
	Configuration []byte `json:"configuration"`
	Checksum      string `json:"checksum"`
}

type ConsumerBackup struct {
	Name   string                `json:"name"`
	Stream string                `json:"stream"`
	Config server.ConsumerConfig `json:"config"`
}

// BackupJetStreamConfiguration creates a backup of all configuration for Streams, Consumers and Stream Templates
func BackupJetStreamConfiguration(backupDir string) error {
	_, err := os.Stat(backupDir)
	if err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("%s already exist", backupDir)
	}

	err = os.MkdirAll(backupDir, 0750)
	if err != nil {
		return err
	}

	log.Printf("Creating JetStream backup into %s", backupDir)
	err = EachStream(func(stream *Stream) {
		err = backupStream(stream, backupDir)
		if err != nil {
			log.Fatalf("Could not backup Stream %s: %s", stream.Name(), err)
		}
	})
	if err != nil {
		return err
	}

	err = EachStreamTemplate(func(template *StreamTemplate) {
		err = backupStreamTemplate(template, backupDir)
		if err != nil {
			log.Fatalf("Could not backup Stream Template %s: %s", template.Name(), err)
		}
	})

	log.Printf("Configuration backup complete")

	return nil
}

// RestoreJetStreamConfiguration restores the configuration from a backup made by BackupJetStreamConfiguration
func RestoreJetStreamConfiguration(backupDir string) error {
	backups := []*BackupData{}

	// load all backups files since we have to do them in a specific order
	err := filepath.Walk(backupDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(info.Name(), ".json") {
			return nil
		}

		log.Printf("Reading file %s", path)
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		bd := &BackupData{}
		err = json.Unmarshal(b, bd)
		if err != nil {
			return err
		}

		if !verifySum(bd.Configuration, bd.Checksum) {
			return fmt.Errorf("data checksum failed for  %s", path)
		}

		backups = append(backups, bd)

		return nil
	})
	if err != nil {
		return err
	}

	eachOfType := func(bt string, cb func(*BackupData) error) error {
		for _, b := range backups {
			if b.Type == bt {
				err := cb(b)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	err = eachOfType("stream", restoreStream)
	if err != nil {
		return err
	}

	err = eachOfType("stream_template", restoreStreamTemplate)
	if err != nil {
		return err
	}

	err = eachOfType("consumer", restoreConsumer)
	if err != nil {
		return err
	}

	return err
}

// RestoreJetStreamConfigurationFile restores a single file from a backup made by BackupJetStreamConfiguration
func RestoreJetStreamConfigurationFile(path string) error {
	log.Printf("Reading file %s", path)
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	bd := &BackupData{}
	err = json.Unmarshal(b, bd)
	if err != nil {
		return err
	}

	if !verifySum(bd.Configuration, bd.Checksum) {
		return fmt.Errorf("data checksum failed for %s", path)
	}

	switch bd.Type {
	case "stream":
		err = restoreStream(bd)
	case "consumer":
		err = restoreConsumer(bd)
	case "stream_template":
		err = restoreStreamTemplate(bd)
	default:
		err = fmt.Errorf("unknown backup type %q", bd.Type)
	}

	return err
}

func restoreStream(backup *BackupData) error {
	if backup.Type != "stream" {
		return fmt.Errorf("cannot restore backup of type %q as Stream", backup.Type)
	}

	sc := server.StreamConfig{}
	err := json.Unmarshal([]byte(backup.Configuration), &sc)
	if err != nil {
		return err
	}

	if sc.Template != "" {
		log.Printf("Skipping Template managed Stream %s", sc.Name)
		return nil
	}

	log.Printf("Restoring Stream %s", sc.Name)
	_, err = NewStreamFromDefault(sc.Name, sc)
	return err
}

func restoreStreamTemplate(backup *BackupData) error {
	if backup.Type != "stream_template" {
		return fmt.Errorf("cannot restore backup of type %q as Stream Template", backup.Type)
	}

	tc := server.StreamTemplateConfig{}
	err := json.Unmarshal([]byte(backup.Configuration), &tc)
	if err != nil {
		return err
	}

	tc.Config.Name = ""

	log.Printf("Restoring Stream Template %s", tc.Name)
	_, err = NewStreamTemplate(tc.Name, tc.MaxStreams, *tc.Config)
	return err
}

func restoreConsumer(backup *BackupData) error {
	if backup.Type != "consumer" {
		return fmt.Errorf("cannot restore backup of type %q as Consumer", backup.Type)
	}

	cc := ConsumerBackup{}
	err := json.Unmarshal([]byte(backup.Configuration), &cc)
	if err != nil {
		return err
	}

	log.Printf("Restoring Consumer %s", cc.Name)
	_, err = NewConsumerFromDefault(cc.Stream, cc.Config)
	return err
}

func backupStream(stream *Stream, backupDir string) error {
	path := filepath.Join(backupDir, fmt.Sprintf("stream_%s.json", stream.Name()))
	log.Printf("Stream %s to %s", stream.Name(), path)

	bupj, err := backupSerialize(stream.Configuration(), "stream")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path, bupj, 0640)
	if err != nil {
		return err
	}

	err = stream.EachConsumer(func(consumer *Consumer) {
		err = backupConsumer(consumer, backupDir)
		if err != nil {
			return
		}
	})

	return err
}

func backupStreamTemplate(template *StreamTemplate, backupDir string) error {
	path := filepath.Join(backupDir, fmt.Sprintf("stream_template_%s.json", template.Name()))
	log.Printf("Stream Template %s to %s", template.Name(), path)

	bupj, err := backupSerialize(template.Configuration(), "stream_template")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bupj, 0640)
}

func backupConsumer(consumer *Consumer, backupDir string) error {
	if consumer.IsEphemeral() {
		log.Printf("Consumer %s > %s skipped", consumer.StreamName(), consumer.Name())
		return nil
	}

	path := filepath.Join(backupDir, fmt.Sprintf("stream_%s_consumer_%s.json", consumer.StreamName(), consumer.Name()))
	log.Printf("Consumer %s > %s to %s", consumer.StreamName(), consumer.Name(), path)

	cb := &ConsumerBackup{
		Name:   consumer.Name(),
		Stream: consumer.StreamName(),
		Config: consumer.Configuration(),
	}

	bupj, err := backupSerialize(cb, "consumer")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bupj, 0640)
}

func verifySum(data []byte, csum string) bool {
	sum := sha256.New()
	sum.Write(data)

	return fmt.Sprintf("%x", sum.Sum(nil)) == csum
}

func backupSerialize(data interface{}, btype string) ([]byte, error) {
	dj, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, err
	}

	sum := sha256.New()
	sum.Write(dj)

	backup := &BackupData{
		Type:          btype,
		Time:          time.Now().UTC().Format(time.RFC3339),
		Configuration: dj,
		Checksum:      fmt.Sprintf("%x", sum.Sum(nil)),
	}

	return json.MarshalIndent(backup, "", "  ")
}
