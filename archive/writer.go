package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"archive/zip"
)

type Writer struct {
	path        string
	fileWriter  *os.File
	zipWriter   *zip.Writer
	manifestMap map[string][]*Tag
}

func (w *Writer) Close() error {

	// Add manifest file to archive before closing it
	if w.zipWriter != nil && w.fileWriter != nil {
		err := w.Add(w.manifestMap, internalTagManifest())
		if err != nil {
			return fmt.Errorf("failed to add manifest")
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

// AddArtifact is a low-level API that adds bytes without adding to the index.
// In most cases, don't use this and use Add instead.
func (w *Writer) AddArtifact(name string, content *bytes.Reader) error {
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

// Add serializes the given artifact and adds it to the archive, it creates a file name based on the provided tags
// and ensures uniqueness. The artifact is also added to the manifest for indexing, enabling tag-based querying
// in the reader
func (w *Writer) Add(artifact any, tags ...*Tag) error {
	// Create filename based on tags
	name, err := createFilenameFromTags(tags)
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

	// Encode the artifact as (pretty-formatted) JSON
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(artifact)
	if err != nil {
		return fmt.Errorf("failed to encode: %w", err)
	}

	// Add file and its tags to the manifest
	w.manifestMap[name] = tags

	return nil
}

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
