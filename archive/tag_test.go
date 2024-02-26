package archive

import (
	"testing"
)

func Test_createFilenameFromTags(t *testing.T) {

	tests := []struct {
		name    string
		tags    []*Tag
		want    string
		wantErr bool
	}{
		{
			"server health",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagHealth()},
			"artifact__cluster_C1__server_S1__health.json",
			false,
		},
		{
			"server info",
			[]*Tag{TagCluster("C1"), TagServer("S1"), TagServerVars()},
			"artifact__cluster_C1__server_S1__variables.json",
			false,
		},
		{
			"cluster info",
			[]*Tag{TagCluster("C1"), TagArtifactType("cluster_info")},
			"artifact__cluster_C1__cluster_info.json",
			false,
		},
		{
			"account connections",
			[]*Tag{TagAccount("A1"), TagConnections()},
			"artifact__account_A1__connections.json",
			false,
		},
		{
			"custom type",
			[]*Tag{TagAccount("A1"), TagArtifactType("snapshot")},
			"artifact__account_A1__snapshot.json",
			false,
		},
		{
			"manifest",
			[]*Tag{internalTagManifest()},
			"manifest.json",
			false,
		},
		{
			"manifest with other tag",
			[]*Tag{internalTagManifest(), TagServer("foo")},
			"",
			true,
		},
		{
			"no tags",
			[]*Tag{},
			"",
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createFilenameFromTags(tt.tags)
			if (err != nil) != tt.wantErr {
				t.Errorf("createFilenameFromTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("createFilenameFromTags() got = %v, want %v", got, tt.want)
			}
		})
	}
}
