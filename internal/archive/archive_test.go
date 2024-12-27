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
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func Test_CreateThenReadArchive(t *testing.T) {
	rng := rand.New(rand.NewSource(123456))

	archivePath := filepath.Join(t.TempDir(), "archive.zip")
	aw, err := NewWriter(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	files := map[string][]byte{
		"empty_file.txt": make([]byte, 0),
		"2KB_file.bin":   make([]byte, 2048),
		"2MB_file.bin":   make([]byte, 2048*1024),
	}

	for fileName, fileContent := range files {
		_, err = rng.Read(fileContent)
		if err != nil {
			t.Fatalf("Failed to generate random file contents: %s", err)
		}
		err = aw.addArtifact(fileName, bytes.NewReader(fileContent))
		if err != nil {
			t.Fatalf("Failed to add file '%s': %s", fileName, err)
		}
	}

	err = aw.Close()
	if err != nil {
		t.Fatalf("Error closing writer: %s", err)
	}

	fileInfo, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("Failed to get archive stats: %s", err)
	}
	t.Logf("Archive file size: %d KiB", fileInfo.Size()/1024)

	ar, err := NewReader(archivePath)
	defer func(ar *Reader) {
		err := ar.Close()
		if err != nil {
			t.Logf("Archive close error: %s", err)
		}
	}(ar)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	expectedArtifactsCount := len(files) + 1 // (+1 for manifest)
	if expectedArtifactsCount != ar.rawFilesCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.rawFilesCount())
	}

	for fileName, fileContent := range files {
		fileReader, size, err := ar.getFileReader(fileName)
		if err != nil {
			t.Fatalf("Failed to get file: %s: %s", fileName, err)
		}
		defer fileReader.Close()

		if uint64(len(fileContent)) != size {
			t.Fatalf("File %s size mismatch: %d vs. %d", fileName, len(fileContent), size)
		}

		buf, err := io.ReadAll(fileReader)
		if err != nil {
			t.Fatalf("Failed to read content of %s: %s", fileName, err)
		}

		if !bytes.Equal(fileContent, buf) {
			t.Fatalf("File %s content mismatch", fileName)
		}

		t.Logf("Verified file %s, uncompressed size: %dB", fileName, size)
	}
}

func Test_CreateThenReadArchiveUsingTags(t *testing.T) {
	rng := rand.New(rand.NewSource(123456))

	archivePath := filepath.Join(t.TempDir(), "archive.zip")
	aw, err := NewWriter(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	clusters := map[string][]string{
		"C1": {
			"X",
			"Y",
			"Z",
		},
		"C2": {
			"A",
			"B",
			"C",
			"D",
			"E",
		},
	}

	type DummyRecord struct {
		FooString string
		BarInt    int
		BazBytes  []byte
	}

	type DummyHealthStats DummyRecord
	type DummyClusterInfo DummyRecord
	type DummyServerInfo DummyRecord
	type DummyStreamInfo DummyRecord
	type DummyAccountInfo DummyRecord

	expectedClusters := make([]string, 0, 2)
	expectedServers := make([]string, 0, 8)

	for clusterName, clusterServers := range clusters {
		expectedClusters = append(expectedClusters, clusterName)

		var err error
		// Add one (dummy) cluster info for each cluster
		ci := &DummyClusterInfo{
			FooString: clusterName,
			BarInt:    rng.Int(),
			BazBytes:  make([]byte, 100),
		}
		rng.Read(ci.BazBytes)
		err = aw.Add(ci, TagCluster(clusterName), TagServer(clusterServers[0]), TagArtifactType("cluster_info"))
		if err != nil {
			t.Fatalf("Failed to add cluster info: %s", err)
		}

		for _, serverName := range clusterServers {
			expectedServers = append(expectedServers, serverName)

			// Add one (dummy) health stats for each server
			hs := &DummyHealthStats{
				FooString: serverName,
				BarInt:    rng.Int(),
				BazBytes:  make([]byte, 50),
			}
			rng.Read(hs.BazBytes)

			err = aw.Add(hs, TagCluster(clusterName), TagServer(serverName), TagServerHealth())
			if err != nil {
				t.Fatalf("Failed to add server health: %s", err)
			}

			// Add one (dummy) server info for each server
			si := &DummyServerInfo{
				FooString: serverName,
				BarInt:    rng.Int(),
				BazBytes:  make([]byte, 50),
			}
			rng.Read(si.BazBytes)

			err = aw.Add(si, TagCluster(clusterName), TagServer(serverName), TagArtifactType("server_info"))
			if err != nil {
				t.Fatalf("Failed to add server health: %s", err)
			}
		}
	}
	slices.Sort(expectedClusters)
	slices.Sort(expectedServers)

	// Add account info
	globalAccountName := "$G"
	for _, serverName := range clusters["C1"] {

		si := &DummyAccountInfo{
			FooString: globalAccountName,
			BarInt:    rng.Int(),
			BazBytes:  make([]byte, 50),
		}
		rng.Read(si.BazBytes)
		err = aw.Add(si, TagAccount(globalAccountName), TagCluster("C1"), TagServer(serverName), TagArtifactType("account_info"))
		if err != nil {
			t.Fatalf("Failed to add account info: %s", err)
		}
	}

	// Add some stream artifacts
	streamName := "ORDERS"
	streamAccount := globalAccountName
	streamReplicas := []string{"A", "B", "E"}
	for _, streamReplicaServerName := range streamReplicas {
		// Add one (dummy) health stats for each server
		si := &DummyStreamInfo{
			FooString: streamAccount + "_" + streamName + "_" + streamReplicaServerName,
			BarInt:    rng.Int(),
			BazBytes:  make([]byte, 50),
		}
		rng.Read(si.BazBytes)

		tags := []*Tag{
			TagAccount(streamAccount),
			TagServer(streamReplicaServerName),
			TagStream(streamName),
			TagArtifactType("stream_info"),
			TagCluster("C2"),
		}

		err = aw.Add(si, tags...)
		if err != nil {
			t.Fatalf("Failed to add stream info: %s", err)
		}
	}

	expectedMessageBytes := []byte("Hello World!")
	err = aw.AddRaw(bytes.NewReader(expectedMessageBytes), "txt", TagSpecial("message"))
	if err != nil {
		t.Fatalf("Failed to raw artifact: %s", err)
	}

	err = aw.Close()
	if err != nil {
		t.Fatalf("Error closing writer: %s", err)
	}

	fileInfo, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("Failed to get archive stats: %s", err)
	}
	t.Logf("Archive file size: %d KiB", fileInfo.Size()/1024)

	ar, err := NewReader(archivePath)
	defer func(ar *Reader) {
		err := ar.Close()
		if err != nil {
			t.Logf("Archive close error: %s", err)
		}
	}(ar)
	if err != nil {
		t.Fatalf("Failed to open archive: %s", err)
	}

	expectedFilesList := []string{
		// Server health
		"capture/clusters/C1/X/health.json",
		"capture/clusters/C1/Y/health.json",
		"capture/clusters/C1/Z/health.json",
		"capture/clusters/C2/A/health.json",
		"capture/clusters/C2/B/health.json",
		"capture/clusters/C2/C/health.json",
		"capture/clusters/C2/D/health.json",
		"capture/clusters/C2/E/health.json",

		// Server info
		"capture/clusters/C1/X/server_info.json",
		"capture/clusters/C1/Y/server_info.json",
		"capture/clusters/C1/Z/server_info.json",
		"capture/clusters/C2/A/server_info.json",
		"capture/clusters/C2/B/server_info.json",
		"capture/clusters/C2/C/server_info.json",
		"capture/clusters/C2/D/server_info.json",
		"capture/clusters/C2/E/server_info.json",

		// Cluster info
		"capture/clusters/C1/X/cluster_info.json",
		"capture/clusters/C2/A/cluster_info.json",

		// Stream info
		"capture/accounts/$G/streams/ORDERS/replicas/C2__A/stream_info.json",
		"capture/accounts/$G/streams/ORDERS/replicas/C2__B/stream_info.json",
		"capture/accounts/$G/streams/ORDERS/replicas/C2__E/stream_info.json",

		// Account info
		"capture/accounts/$G/servers/C1__X/account_info.json",
		"capture/accounts/$G/servers/C1__Y/account_info.json",
		"capture/accounts/$G/servers/C1__Z/account_info.json",

		// Misc
		"capture/misc/message.txt",
	}
	expectedArtifactsCount := len(expectedFilesList) + 1 // +1 for manifest
	if expectedArtifactsCount != ar.rawFilesCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.rawFilesCount())
	}

	t.Logf("Listing archive contents:")
	for fileName := range ar.filesMap {
		t.Logf(" - %s", fileName)
	}

	for _, fileName := range expectedFilesList {
		if fileName == "capture/misc/message.txt" {
			// Don't try to deserialize text file
			continue
		}
		var r DummyRecord
		err := ar.loadFile(fileName, &r)
		if err != nil {
			t.Fatalf("Failed to load artifact: %s: %s", fileName, err)
		}
		//t.Logf("%s: %+v", fileName, r)
		if r.FooString == "" {
			t.Fatalf("Unexpected empty structure field for file %s", fileName)
		}
	}

	fileReader, _, err := ar.getFileReader("capture/misc/message.txt")
	if err != nil {
		t.Fatalf("Failed to open message file reader: %s", err)
	}
	messageBytes, err := io.ReadAll(fileReader)
	if err != nil {
		t.Fatalf("Failed to read message: %s", err)
	}
	if !bytes.Equal(messageBytes, expectedMessageBytes) {
		t.Fatalf("Expected message: %s, actual: %s", expectedMessageBytes, messageBytes)
	}

	uniqueAccountTags := ar.accountTags
	if len(uniqueAccountTags) != 1 {
		t.Fatalf("Expected 1 accounts, got %d: %v", len(uniqueAccountTags), uniqueAccountTags)
	} else if uniqueAccountTags[0].Value != globalAccountName {
		t.Fatalf("Expected account name %s, got %s", globalAccountName, uniqueAccountTags[0].Value)
	}

	uniqueClusterTags := ar.clusterTags
	if len(expectedClusters) != len(uniqueClusterTags) {
		t.Fatalf("Expected %d clusters, got %d: %v", len(expectedClusters), len(uniqueClusterTags), uniqueClusterTags)
	}

	uniqueServerTags := ar.serverTags
	if len(expectedServers) != len(uniqueServerTags) {
		t.Fatalf("Expected %d servers, got %d: %v", len(expectedServers), len(uniqueServerTags), uniqueServerTags)
	}

	for _, serverTag := range uniqueServerTags {
		var si DummyServerInfo
		err := ar.Load(&si, &serverTag, TagArtifactType("server_info"))
		if err != nil {
			t.Fatalf("Failed to load server info artifact for server %s: %s", serverTag.Value, err)
		}
		if serverTag.Value != si.FooString {
			t.Fatalf("Unexpected value '%s' (should be: '%s')", si.FooString, serverTag.Value)
		}
	}

	clusterNames := ar.GetClusterNames()
	if slices.Compare(clusterNames, expectedClusters) != 0 {
		t.Fatalf("Expected clusters: %v, got: %v", expectedClusters, clusterNames)
	}

	for _, clusterName := range clusterNames {
		serverNames := ar.GetClusterServerNames(clusterName)
		if slices.Compare(clusters[clusterName], serverNames) != 0 {
			t.Fatalf("Expected cluster %s servers: %v, got: %v", clusterName, clusters[clusterName], serverNames)
		}
	}

	expectedAccountNames := []string{globalAccountName}
	accountNames := ar.GetAccountNames()
	if slices.Compare(expectedAccountNames, accountNames) != 0 {
		t.Fatalf("Expected accounts: %v, got: %v", expectedAccountNames, accountNames)
	}

	expectedStreamNames := []string{"ORDERS"}
	streamNames := ar.GetAccountStreamNames(globalAccountName)
	if slices.Compare(expectedStreamNames, streamNames) != 0 {
		t.Fatalf("Expected account %s streams: %v, got: %v", globalAccountName, expectedStreamNames, streamNames)
	}

	expectedReplicaNames := []string{"A", "B", "E"}
	replicaNames := ar.GetStreamServerNames(globalAccountName, "ORDERS")
	if slices.Compare(expectedReplicaNames, replicaNames) != 0 {
		t.Fatalf("Expected stream %s/%s replicas: %v, got: %v", globalAccountName, "ORDERS", expectedReplicaNames, replicaNames)
	}

	var foo struct{}
	if err = ar.Load(&foo, TagCluster("C1"), TagServer("A")); !errors.Is(err, ErrNoMatches) {
		t.Fatalf("Expected error '%s', but got: '%s'", ErrNoMatches, err)
	}
	if err = ar.Load(&foo, TagServerHealth()); !errors.Is(err, ErrMultipleMatches) {
		t.Fatalf("Expected error '%s', but got: '%s'", ErrMultipleMatches, err)
	}
}

func Test_IterateResourcesUsingTags(t *testing.T) {
	rng := rand.New(rand.NewSource(123456))

	dummyArtifact := struct {
		x int
		y []byte
	}{
		x: rng.Int(),
	}
	rng.Read(dummyArtifact.y)

	archivePath := filepath.Join(t.TempDir(), "archive.zip")
	aw, err := NewWriter(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	clusterServerMap := map[string][]string{
		"C1": {"A", "B", "C"},
		"C2": {"X", "Y", "Z"},
	}

	expectedClusterNames := []string{
		"C1",
		"C2",
	}
	slices.SortFunc(expectedClusterNames, strings.Compare)

	for clusterName, serverNames := range clusterServerMap {
		for _, serverName := range serverNames {
			err = aw.Add(
				dummyArtifact,
				TagCluster(clusterName),
				TagServer(serverName),
				TagServerHealth(),
			)
			if err != nil {
				t.Fatalf("Failed to add artifact: %s", err)
			}
		}
	}

	err = aw.Close()
	if err != nil {
		t.Fatalf("Error closing writer: %s", err)
	}

	// Done writing, now verify

	ar, err := NewReader(archivePath)
	defer func(ar *Reader) {
		err := ar.Close()
		if err != nil {
			t.Logf("Failed to close reader: %s", err)
		}
	}(ar)
	if err != nil {
		t.Fatalf("Failed to open archive: %s", err)
	}

	clusterNames := ar.GetClusterNames()
	slices.SortFunc(clusterNames, strings.Compare)

	if !slices.Equal(clusterNames, expectedClusterNames) {
		t.Fatalf("Expected clusters: %v, actual: %v", expectedClusterNames, clusterNames)
	}

	if len(ar.GetClusterServerNames("NO_SUCH_CLUSTER")) != 0 {
		t.Fatalf("Looking up non-existent cluster produced some results")
	}

	for clusterName, expectedServerNames := range clusterServerMap {
		serverNames := ar.GetClusterServerNames(clusterName)
		slices.SortFunc(expectedServerNames, strings.Compare)
		slices.SortFunc(serverNames, strings.Compare)
		if !slices.Equal(serverNames, expectedServerNames) {
			t.Fatalf("Expected cluster %s servers: %v, actual: %v", clusterName, expectedServerNames, serverNames)
		}
	}
}

// TODO test writer overwrites existing file
// TODO test creation in non-existing directory fails
// TODO test adding twice a file with the same name (or tags)
// TODO test with non-unique server name in different clusters
