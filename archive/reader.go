package archive

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	archiveReader       *zip.ReadCloser
	path                string
	filesMap            map[string]*zip.File
	manifestMap         map[string][]Tag
	accountTags         []Tag
	clusterTags         []Tag
	serverTags          []Tag
	streamTags          []Tag
	accountNames        []string
	clusterNames        []string
	clustersServerNames map[string][]string
	accountStreamNames  map[string][]string
	streamServerNames   map[string][]string
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

var ErrNoMatches = fmt.Errorf("no file matched the given query")
var ErrMultipleMatches = fmt.Errorf("multiple files matched the given query")

// Load queries the manifest and looking for a single matching artifact for the given query (conjunction of tags).
// If a single artifact is found, then it is deserialized into v
func (r *Reader) Load(v any, queryTags ...*Tag) error {

	//TODO querying scans the entire manifest every time. Probably ok for now, but may get noticeably slow for very
	// large archives, or large number of checks.
	// A simple inverted index would be the right approach, eventually. For now this will do.

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
		} else if fileName == captureLogName {
			// Capture log is not present in manifest
			continue
		} else if fileName == metadataName {
			// Metadata file is not present in manifest
			continue
		}
		if _, present := manifestMap[fileName]; !present {
			fmt.Printf("Warning: archive file %s is not present in manifest\n", fileName)
		}
	}

	// Map of cluster to set of server names
	clustersServersMap := make(map[string]map[string]interface{})
	accountsStreamsMap := make(map[string]map[string]map[string]interface{})

	for _, tags := range manifestMap {
		// Take note of certain tags, if present
		var cluster, server, account, stream string
		for _, tag := range tags {
			switch tag.Name {
			case clusterTagLabel:
				cluster = tag.Value
			case serverTagLabel:
				server = tag.Value
			case accountTagLabel:
				account = tag.Value
			case streamTagLabel:
				stream = tag.Value
			}
		}

		// If a cluster tag is set, create a record for it
		if cluster != "" {
			if _, knownCluster := clustersServersMap[cluster]; !knownCluster {
				clustersServersMap[cluster] = make(map[string]interface{})
			}
			// File has cluster and server tags, save server in set for this cluster
			if server != "" {
				clustersServersMap[cluster][server] = nil // Map used as set, value doesn't matter
			}
		}

		// If an account tag is set, create a record for it
		if account != "" {
			if _, knownAccount := accountsStreamsMap[account]; !knownAccount {
				accountsStreamsMap[account] = make(map[string]map[string]interface{})
			}
			// If account and stream tags present, save stream in set for this account
			if stream != "" {
				if _, knownStream := accountsStreamsMap[account][stream]; !knownStream {
					accountsStreamsMap[account][stream] = make(map[string]interface{})
				}
				// If account and stream and server tags present, save server in set for this stream
				if server != "" {
					accountsStreamsMap[account][stream][server] = nil // Map used as set, value doesn't matter
				}
			}
		}
	}

	clusters, clusterServers := shrinkMapOfSets(clustersServersMap)
	accounts, accountsStreams := shrinkMapOfSets(accountsStreamsMap)
	streamsServers := make(map[string][]string, len(accounts))
	for account, streamsMapServersSet := range accountsStreamsMap {
		_, streamServers := shrinkMapOfSets(streamsMapServersSet)
		for stream, serversList := range streamServers {
			key := account + "/" + stream
			streamsServers[key] = serversList
		}
	}

	// Returns a deduplicated list of tags for the specific label present in the archive
	// e.g. getUniqueTags(serverTagLabel) -> [Tag(server, s1), Tag(server, s2, Tag(server, s3)]
	// TODO each call scans the manifest, could actually do everything in a single pass
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
		path:                archivePath,
		archiveReader:       archiveReader,
		filesMap:            filesMap,
		manifestMap:         manifestMap,
		accountTags:         getUniqueTags(accountTagLabel),
		clusterTags:         getUniqueTags(clusterTagLabel),
		serverTags:          getUniqueTags(serverTagLabel),
		streamTags:          getUniqueTags(streamTagLabel),
		accountNames:        accounts,
		clusterNames:        clusters,
		clustersServerNames: clusterServers,
		accountStreamNames:  accountsStreams,
		streamServerNames:   streamsServers,
	}, nil
}

// Given a map[string] of sets (map[string]any), return:
// The list of (unique) keys as string slice
// A shrunk map replacing a set with a list, i.e. map[string][]string
func shrinkMapOfSets[T any](m map[string]map[string]T) ([]string, map[string][]string) {
	keysList := make([]string, 0, len(m))
	newMap := make(map[string][]string, len(m))
	for k, valuesMap := range m {
		keysList = append(keysList, k)
		newMap[k] = make([]string, 0, len(valuesMap))
		for value, _ := range valuesMap {
			newMap[k] = append(newMap[k], value)
		}
	}
	return keysList, newMap
}

// ListStreamTags returns a list of unique stream tags attached to files in the archive.
func (r *Reader) ListStreamTags() []Tag {
	return shallowCopy(r.streamTags)
}

// ListServerTags returns a list of unique server tags attached to files in the archive.
// Keep in mind server names may not be unique across clusters!
func (r *Reader) ListServerTags() []Tag {
	return shallowCopy(r.serverTags)
}

// ListClusterTags returns a list of unique cluster tags attached to files in the archive.
func (r *Reader) ListClusterTags() []Tag {
	return shallowCopy(r.clusterTags)
}

// ListAccountTags returns a list of unique account tags attached to files in the archive.
func (r *Reader) ListAccountTags() []Tag {
	return shallowCopy(r.accountTags)
}

func (r *Reader) GetAccountNames() []string {
	return shallowCopy(r.accountNames)
}

func (r *Reader) GetAccountStreamNames(accountName string) []string {
	streams, present := r.accountStreamNames[accountName]
	if present {
		return shallowCopy(streams)
	}
	return make([]string, 0)
}

func (r *Reader) GetClusterNames() []string {
	return shallowCopy(r.clusterNames)
}

func (r *Reader) GetClusterServerNames(clusterName string) []string {
	servers, present := r.clustersServerNames[clusterName]
	if present {
		return shallowCopy(servers)
	}
	return make([]string, 0)
}

func (r *Reader) GetStreamServerNames(accountName, streamName string) []string {
	servers, present := r.streamServerNames[accountName+"/"+streamName]
	if present {
		return shallowCopy(servers)
	}
	return make([]string, 0)
}

// copyOfNamesList creates a shallow copy of the given list of strings
// This avoids the risk of the internal indices list being modified by some receiver of the list.
func shallowCopy[T any](original []T) []T {
	return append(make([]T, 0, len(original)), original...)
}
