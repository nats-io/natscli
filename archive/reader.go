package archive

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	archiveReader *zip.ReadCloser
	path          string
	filesMap      map[string]*zip.File
	manifestMap   map[string][]Tag
	accountTags   []Tag
	clusterTags   []Tag
	serverTags    []Tag
	streamTags    []Tag
}

func (r *Reader) rawFilesCount() int {
	return len(r.archiveReader.File)
}

func (r *Reader) Close() error {
	if r.archiveReader != nil {
		err := r.archiveReader.Close()
		r.archiveReader = nil
		return err
	}
	return nil
}

// GetFile is a low-level API that returns a reader for the given filename, if it exists in the archive.
// In most cases you should use Get or Load
func (r *Reader) GetFile(name string) (io.ReadCloser, uint64, error) {
	f, exists := r.filesMap[name]
	if !exists {
		return nil, 0, os.ErrNotExist
	}
	reader, err := f.Open()
	if err != nil {
		return nil, 0, err
	}
	return reader, f.UncompressedSize64, nil
}

// Get decodes the provided filename into the given value
func (r *Reader) Get(name string, v any) error {
	f, _, err := r.GetFile(name)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(f)
	err = decoder.Decode(v)
	if err != nil {
		return fmt.Errorf("failed to decode: %w", err)
	}
	return nil
}

// ListStreamTags returns a list of unique stream tags attached to files in the archive.
func (r *Reader) ListStreamTags() []Tag {
	return r.streamTags
}

// ListServerTags returns a list of unique server tags attached to files in the archive.
// Keep in mind server names may not be unique across clusters!
func (r *Reader) ListServerTags() []Tag {
	return r.serverTags
}

// ListClusterTags returns a list of unique cluster tags attached to files in the archive.
func (r *Reader) ListClusterTags() []Tag {
	return r.clusterTags
}

// ListAccountTags returns a list of unique account tags attached to files in the archive.
func (r *Reader) ListAccountTags() []Tag {
	return r.accountTags
}

var ErrNoMatches = fmt.Errorf("no file matched the given query")
var ErrMultipleMatches = fmt.Errorf("multiple files matched the given query")

// Load queries the manifest and is expected to find a single matching artifact, which is loaded into the given value.
func (r *Reader) Load(v any, queryTags ...*Tag) error {

	//TODO this scanning is inefficient, could be made better by creating inverted indices during reader open

	matchedFileNames := make([]string, 0, 1)

	// Find manifest entry that matches all given query tags
manifestSearchLoop:
	for fileName, fileTags := range r.manifestMap {
		// Turn file tags into a set
		fileTagSet := make(map[Tag]struct{}, len(fileTags))
		for _, fileTag := range fileTags {
			fileTagSet[fileTag] = struct{}{}
		}

		// Check that each query tag is in this file tag set
		for _, queryTag := range queryTags {
			_, present := fileTagSet[*queryTag]
			if !present {
				continue manifestSearchLoop
			}
		}

		// This file matches
		matchedFileNames = append(matchedFileNames, fileName)

		// Continue iterating and find all matching files
		continue manifestSearchLoop
	}

	if len(matchedFileNames) < 1 {
		return ErrNoMatches
	} else if len(matchedFileNames) > 1 {
		return ErrMultipleMatches
	}

	// A single file matched
	matchedFileName := matchedFileNames[0]

	// Unmarshall it into v
	return r.Get(matchedFileName, v)
}

func NewReader(archivePath string) (*Reader, error) {

	// Create a zip reader
	archiveReader, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open archive: %w", err)
	}

	// Create map of filename -> file
	filesMap := make(map[string]*zip.File, len(archiveReader.File))
	for _, f := range archiveReader.File {
		filesMap[f.Name] = f
	}

	// Find and open the manifest file
	manifestFileName, err := createFilenameFromTags([]*Tag{internalTagManifest()})
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	manifestFile, exists := filesMap[manifestFileName]
	if !exists {
		return nil, fmt.Errorf("manifest file not found in archive")
	}

	manifestFileReader, err := manifestFile.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer manifestFileReader.Close()

	// Load manifest, which is a normalized index:
	// For each file, a list of tags is present
	manifestMap := make(map[string][]Tag, len(filesMap))
	err = json.NewDecoder(manifestFileReader).Decode(&manifestMap)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Check that each file in the manifest exists in the archive
	for fileName, _ := range manifestMap {
		_, present := filesMap[fileName]
		if !present {
			return nil, fmt.Errorf("file %s is in manifest, but not present in archive", fileName)
		}
	}

	// Check that each file in the archive is present in the manifest
	for fileName, _ := range filesMap {
		if fileName == ManifestFileName {
			// Manifest is not present in manifest
			continue
		}
		if _, present := manifestMap[fileName]; !present {
			fmt.Printf("Warning: archive file %s is not present in manifest\n", fileName)
		}
	}

	// Returns a deduplicated list of tags for the specific label present in the archive
	// e.g. getUniqueTags(serverTagLabel) -> [Tag(server, s1), Tag(server, s2, Tag(server, s3)]
	getUniqueTags := func(label TagLabel) []Tag {
		var tagsSet = make(map[Tag]struct{}, len(manifestMap))
		for _, tags := range manifestMap {
			for _, tag := range tags {
				if tag.Name == label {
					// Found a tag for the given label, add it to the set
					tagsSet[tag] = struct{}{}
				}
			}
		}
		// Create list of unique tags from the set
		tagsList := make([]Tag, 0, len(tagsSet))
		for tag, _ := range tagsSet {
			tagsList = append(tagsList, tag)
		}
		return tagsList
	}

	return &Reader{
		path:          archivePath,
		archiveReader: archiveReader,
		filesMap:      filesMap,
		manifestMap:   manifestMap,
		accountTags:   getUniqueTags(accountTagLabel),
		clusterTags:   getUniqueTags(clusterTagLabel),
		serverTags:    getUniqueTags(serverTagLabel),
		streamTags:    getUniqueTags(streamTagLabel),
	}, nil
}
