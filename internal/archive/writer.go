// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package archive

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Writer encapsulates a zip writer for the underlying archive file, but also tracks metadata used by the Reader to
// construct indices
type Writer struct {
	path        string
	fileWriter  *os.File
	zipWriter   *zip.Writer
	manifestMap map[string][]*Tag
}

// Close closes the writer
func (w *Writer) Close() error {
	// Add manifest file to archive before closing it
	if w.zipWriter != nil && w.fileWriter != nil {
		err := w.Add(w.manifestMap, internalTagManifest())
		if err != nil {
			return fmt.Errorf("failed to add manifest: %w", err)
		}
	}

	// Close and null the zip writer
	if w.zipWriter != nil {
		err := w.zipWriter.Close()
		w.zipWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close archive zip writer: %w", err)
		}
	}

	// Close and null the file writer
	if w.fileWriter != nil {
		err := w.fileWriter.Close()
		w.fileWriter = nil
		if err != nil {
			return fmt.Errorf("failed to close archive file writer: %w", err)
		}
	}

	return nil
}

// addArtifact low-level API that adds bytes without adding to the index, used for special files
func (w *Writer) addArtifact(name string, content *bytes.Reader) error {
	f, err := w.zipWriter.Create(name)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, content)
	if err != nil {
		return err
	}

	return nil
}

// Add serializes the given artifact to JSON and adds it to the archive, it creates a file name based on the provided
// tags and ensures uniqueness. The artifact is also added to the manifest for indexing, enabling tag-based querying
// in via Reader
func (w *Writer) Add(artifact any, tags ...*Tag) error {
	// Encode the artifact as (pretty-formatted) JSON
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	err := encoder.Encode(artifact)
	if err != nil {
		return fmt.Errorf("failed to encode: %w", err)
	}

	return w.AddRaw(bytes.NewReader(buf.Bytes()), "json", tags...)
}

// AddRaw adds the given artifact to the archive similarly to Add.
// The artifact is assumed to be already serialized and is copied as-is byte for byte.
func (w *Writer) AddRaw(reader *bytes.Reader, extension string, tags ...*Tag) error {
	if w.zipWriter == nil {
		return fmt.Errorf("attempting to write into a closed writer")
	}

	// Create filename based on tags
	name, err := createFilenameFromTags(extension, tags)
	if err != nil {
		return fmt.Errorf("failed to create artifact name: %w", err)
	}

	// Ensure file is unique
	_, exists := w.manifestMap[name]
	if exists {
		return fmt.Errorf("artifact %s with identical tags is already present", name)
	}

	// Open a zip writer
	f, err := w.zipWriter.Create(name)
	if err != nil {
		return fmt.Errorf("failed to create file in archive: %w", err)
	}

	_, err = io.Copy(f, reader)
	if err != nil {
		return fmt.Errorf("failed to copy content: %w", err)
	}

	// Add file and its tags to the manifest
	w.manifestMap[name] = tags

	return nil
}

// NewWriter creates a new writer for the file at the given archivePath.
// Writer creates a ZIP file whose content has additional structure and metadata.
// If archivePath is an existing file, it will be overwritten.
func NewWriter(archivePath string) (*Writer, error) {
	fileWriter, err := os.Create(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create archive: %w", err)
	}

	zipWriter := zip.NewWriter(fileWriter)

	return &Writer{
		path:        archivePath,
		fileWriter:  fileWriter,
		zipWriter:   zipWriter,
		manifestMap: make(map[string][]*Tag),
	}, nil
}
