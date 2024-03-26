package archive

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/exp/slices"
)

func Test_CreateThenReadArchive(t *testing.T) {
	const SEED = 123456
	rng := rand.New(rand.NewSource(SEED))

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
		err = aw.AddArtifact(fileName, bytes.NewReader(fileContent))
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
	defer ar.Close()
	if err != nil {
		t.Fatalf("Failed to create archive: %s", err)
	}

	expectedArtifactsCount := len(files) + 1 // (+1 for manifest)
	if expectedArtifactsCount != ar.rawFilesCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.rawFilesCount())
	}

	for fileName, fileContent := range files {

		fileReader, size, err := ar.GetFile(fileName)
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
	const SEED = 123456
	rng := rand.New(rand.NewSource(SEED))

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

			err = aw.Add(hs, TagCluster(clusterName), TagServer(serverName), TagHealth())
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

	// Add account info
	globalAccountName := "$G"
	for _, serverName := range clusters["C1"] {

		si := &DummyAccountInfo{
			FooString: globalAccountName,
			BarInt:    rng.Int(),
			BazBytes:  make([]byte, 50),
		}
		rng.Read(si.BazBytes)
		err = aw.Add(si, TagAccount(globalAccountName), TagServer(serverName), TagArtifactType("account_info"))
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
	defer ar.Close()
	if err != nil {
		t.Fatalf("Failed to open archive: %s", err)
	}

	expectedFilesList := []string{
		// Server health
		"capture/clusters/C1/server_X__health.json",
		"capture/clusters/C1/server_Y__health.json",
		"capture/clusters/C1/server_Z__health.json",
		"capture/clusters/C2/server_A__health.json",
		"capture/clusters/C2/server_B__health.json",
		"capture/clusters/C2/server_C__health.json",
		"capture/clusters/C2/server_D__health.json",
		"capture/clusters/C2/server_E__health.json",

		// Server info
		"capture/clusters/C1/server_X__server_info.json",
		"capture/clusters/C1/server_Y__server_info.json",
		"capture/clusters/C1/server_Z__server_info.json",
		"capture/clusters/C2/server_A__server_info.json",
		"capture/clusters/C2/server_B__server_info.json",
		"capture/clusters/C2/server_C__server_info.json",
		"capture/clusters/C2/server_D__server_info.json",
		"capture/clusters/C2/server_E__server_info.json",

		// Cluster info
		"capture/clusters/C1/server_X__cluster_info.json",
		"capture/clusters/C2/server_A__cluster_info.json",

		// Stream info
		"capture/accounts/$G/streams/ORDERS/server_A__stream_info.json",
		"capture/accounts/$G/streams/ORDERS/server_B__stream_info.json",
		"capture/accounts/$G/streams/ORDERS/server_E__stream_info.json",

		// Account info
		"capture/accounts/$G/server_X__account_info.json",
		"capture/accounts/$G/server_Y__account_info.json",
		"capture/accounts/$G/server_Z__account_info.json",
	}
	expectedArtifactsCount := len(expectedFilesList) + 1 // +1 for manifest
	if expectedArtifactsCount != ar.rawFilesCount() {
		t.Fatalf("Wrong number of artifacts. Expected: %d actual: %d", expectedArtifactsCount, ar.rawFilesCount())
	}

	t.Logf("Listing archive contents:")
	for fileName, _ := range ar.filesMap {
		t.Logf(" - %s", fileName)
	}

	for _, fileName := range expectedFilesList {
		var r DummyRecord
		err := ar.Get(fileName, &r)
		if err != nil {
			t.Fatalf("Failed to load artifact: %s: %s", fileName, err)
		}
		//t.Logf("%s: %+v", fileName, r)
		if r.FooString == "" {
			t.Fatalf("Unexpected empty structure field for file %s", fileName)
		}
	}

	uniqueAccountTags := ar.ListAccountTags()
	if len(uniqueAccountTags) != 1 {
		t.Fatalf("Expected 1 accounts, got %d: %v", len(uniqueAccountTags), uniqueAccountTags)
	} else if uniqueAccountTags[0].Value != globalAccountName {
		t.Fatalf("Expected account name %s, got %s", globalAccountName, uniqueAccountTags[0].Value)
	}

	uniqueClusterTags := ar.ListClusterTags()
	if len(expectedClusters) != len(uniqueClusterTags) {
		t.Fatalf("Expected %d clusters, got %d: %v", len(expectedClusters), len(uniqueClusterTags), uniqueClusterTags)
	}

	uniqueServerTags := ar.ListServerTags()
	if len(expectedServers) != len(uniqueServerTags) {
		t.Fatalf("Expected %d servers, got %d: %v", len(expectedServers), len(uniqueServerTags), uniqueServerTags)
	}

	for _, serverTag := range ar.ListServerTags() {
		var si DummyServerInfo
		err := ar.Load(&si, &serverTag, TagArtifactType("server_info"))
		if err != nil {
			t.Fatalf("Failed to load server info artifact for server %s: %s", serverTag.Value, err)
		}
		if serverTag.Value != si.FooString {
			t.Fatalf("Unexpected value '%s' (should be: '%s')", si.FooString, serverTag.Value)
		}
	}

	var foo struct{}
	if err = ar.Load(&foo, TagCluster("C1"), TagServer("A")); !errors.Is(err, ErrNoMatches) {
		t.Fatalf("Expected error '%s', but got: '%s'", ErrNoMatches, err)
	}
	if err = ar.Load(&foo, TagHealth()); !errors.Is(err, ErrMultipleMatches) {
		t.Fatalf("Expected error '%s', but got: '%s'", ErrMultipleMatches, err)
	}
}

func Test_IterateResourcesUsingTags(t *testing.T) {
	const SEED = 123456
	rng := rand.New(rand.NewSource(SEED))

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
				TagHealth(),
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
	defer ar.Close()
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
