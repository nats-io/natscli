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
	"fmt"
	"path"
)

type TagLabel string

type Tag struct {
	Name  TagLabel
	Value string
}

const (
	serverTagLabel      TagLabel = "server"
	clusterTagLabel     TagLabel = "cluster"
	accountTagLabel     TagLabel = "account"
	streamTagLabel      TagLabel = "stream"
	typeTagLabel        TagLabel = "artifact_type"
	profileNameTagLabel TagLabel = "profile_name"
	specialTagLabel     TagLabel = "special"
)

const (
	// Server artifacts
	healtzArtifactType   = "health"
	varzArtifactType     = "variables"
	connzArtifactType    = "connections"
	routezArtifactType   = "routes"
	gatewayzArtifactType = "gateways"
	leafzArtifactType    = "leafs"
	subzArtifactType     = "subs"
	jszArtifactType      = "jetstream_info"
	accountzArtifactType = "accounts"
	// Account artifacts
	accountConnectionsArtifactType = "account_connections"
	accountLeafsArtifactType       = "account_leafs"
	accountSubsArtifactType        = "account_subs"
	accountJetStreamArtifactType   = "account_jetstream_info"
	accountInfoArtifactType        = "account_info"
	streamDetailsArtifactType      = "stream_info"
	// Other artifacts
	manifestArtifactName = "manifest"
	profileArtifactType  = "profile"
)

const (
	// Placeholder for cluster name for unclustered servers
	noCluster = "unclustered"
	// Archive root directory (so it doesn't explode in the containing directory)
	rootDirectory = "capture"
	// Directory where special artifacts are filed under
	specialFilesDirectory = "misc"
	// Used to join dimensions in a path, for example cluster name and server name
	separator = "__"
)

// Special tags that get composed and combined in the filename
var dimensionTagsNames = map[TagLabel]any{
	accountTagLabel:     nil,
	clusterTagLabel:     nil,
	serverTagLabel:      nil,
	streamTagLabel:      nil,
	typeTagLabel:        nil,
	profileNameTagLabel: nil,
}

func createFilenameFromTags(extension string, tags []*Tag) (string, error) {
	if len(tags) < 1 {
		return "", fmt.Errorf("at least one tag is required")
	} else if len(tags) == 1 && tags[0].Name == specialTagLabel {
		// Special-tagged go into a special subdirectory
		return path.Join(
			rootDirectory,
			specialFilesDirectory,
			tags[0].Value+"."+extension,
		), nil
	}

	// "Dimension" tags:
	// - Can have at most one value
	// - They get combined to produce the file path
	dimensionTagsMap := make(map[TagLabel]*Tag, len(tags))

	// Capture non-dimension tags here (unused for now)
	otherTags := make([]*Tag, 0, len(tags))

	for _, tag := range tags {

		// The 'special' tags should not be mixed with the rest
		if tag.Name == specialTagLabel {
			return "", fmt.Errorf("special tag (value: '%s') should not be combined with other tags", tag.Value)
		}

		// Save dimension tags and other tags
		_, isDimensionTag := dimensionTagsNames[tag.Name]
		_, isDuplicateDimensionTag := dimensionTagsMap[tag.Name]
		if isDimensionTag && isDuplicateDimensionTag {
			return "", fmt.Errorf("multiple values not allowed for tag '%s'", tag.Name)
		} else if isDimensionTag {
			dimensionTagsMap[tag.Name] = tag
		} else {
			otherTags = append(otherTags, tag)
		}
	}

	if len(otherTags) > 0 {
		// For the moment, the 'gather' command is the only user of this, and it is not using custom tags.
		// If we ever open the archiving API beyond, we may need to address this.
		return "", fmt.Errorf("unhandled custom tags")
	}

	accountTag, hasAccountTag := dimensionTagsMap[accountTagLabel], dimensionTagsMap[accountTagLabel] != nil
	clusterTag, hasClusterTag := dimensionTagsMap[clusterTagLabel], dimensionTagsMap[clusterTagLabel] != nil
	serverTag, hasServerTag := dimensionTagsMap[serverTagLabel], dimensionTagsMap[serverTagLabel] != nil
	streamTag, hasStreamTag := dimensionTagsMap[streamTagLabel], dimensionTagsMap[streamTagLabel] != nil
	typeTag, hasTypeTag := dimensionTagsMap[typeTagLabel], dimensionTagsMap[typeTagLabel] != nil
	profileNameTag, hasProfileNameTag := dimensionTagsMap[profileNameTagLabel], dimensionTagsMap[profileNameTagLabel] != nil

	// All artifacts must have a type, source server and source cluster (or "un-clustered")
	for requiredTagName, hasRequiredTag := range map[string]bool{
		"artifact type":  hasTypeTag,
		"source cluster": hasClusterTag,
		"source server":  hasServerTag,
	} {
		if !hasRequiredTag {
			return "", fmt.Errorf("missing required tag: %s", requiredTagName)
		}
	}

	if hasStreamTag {
		// Stream artifact must have account and cluster tag
		if !hasClusterTag || !hasAccountTag {
			return "", fmt.Errorf("stream artifact is missing cluster or account tags")
		}
		return path.Join(
			rootDirectory,
			"accounts",
			accountTag.Value,
			"streams",
			streamTag.Value,
			"replicas",
			clusterTag.Value+separator+serverTag.Value,
			typeTag.Value+"."+extension,
		), nil

	} else if hasAccountTag {
		// Account artifact (but not a stream)
		if !hasClusterTag {
			return "", fmt.Errorf("account artifact is missing cluster tag")
		}
		return path.Join(
			rootDirectory,
			"accounts",
			accountTag.Value,
			"servers",
			clusterTag.Value+separator+serverTag.Value,
			typeTag.Value+"."+extension,
		), nil

	} else if hasServerTag {
		// Server artifact
		clusterName := noCluster
		if hasClusterTag {
			clusterName = clusterTag.Value
		}

		// Handle certain types differently
		switch typeTag.Value {
		case profileArtifactType:
			if !hasProfileNameTag {
				return "", fmt.Errorf("profile artifact is missing profile name")
			}
			return path.Join(
				rootDirectory,
				"profiles",
				clusterName,
				serverTag.Value+separator+profileNameTag.Value+"."+extension,
			), nil

		default:
			return path.Join(
				rootDirectory,
				"clusters",
				clusterName,
				serverTag.Value,
				typeTag.Value+"."+extension,
			), nil
		}

	} else {
		// May add more cases later, for now bomb if none of the above applies
		return "", fmt.Errorf("unhandled tags combination: %+v", dimensionTagsMap)
	}
}

func TagArtifactType(artifactType string) *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: artifactType,
	}
}

func TagServerHealth() *Tag {
	return TagArtifactType(healtzArtifactType)
}
func TagServerVars() *Tag {
	return TagArtifactType(varzArtifactType)
}

func TagServerConnections() *Tag {
	return TagArtifactType(connzArtifactType)
}

func TagServerRoutes() *Tag {
	return TagArtifactType(routezArtifactType)
}

func TagServerGateways() *Tag {
	return TagArtifactType(gatewayzArtifactType)
}

func TagServerLeafs() *Tag {
	return TagArtifactType(leafzArtifactType)
}

func TagServerSubs() *Tag {
	return TagArtifactType(subzArtifactType)
}

func TagServerJetStream() *Tag {
	return TagArtifactType(jszArtifactType)
}

func TagServerAccounts() *Tag {
	return TagArtifactType(accountzArtifactType)
}

func TagAccountConnections() *Tag {
	return TagArtifactType(accountConnectionsArtifactType)
}

func TagAccountLeafs() *Tag {
	return TagArtifactType(accountLeafsArtifactType)
}

func TagAccountSubs() *Tag {
	return TagArtifactType(accountSubsArtifactType)
}

func TagAccountJetStream() *Tag {
	return TagArtifactType(accountJetStreamArtifactType)
}

func TagAccountInfo() *Tag {
	return TagArtifactType(accountInfoArtifactType)
}

func TagStreamInfo() *Tag { return TagArtifactType(streamDetailsArtifactType) }

func internalTagManifest() *Tag {
	return TagSpecial(manifestArtifactName)
}

func TagServer(serverName string) *Tag {
	return &Tag{
		Name:  serverTagLabel,
		Value: serverName,
	}
}

func TagCluster(clusterName string) *Tag {
	return &Tag{
		Name:  clusterTagLabel,
		Value: clusterName,
	}
}

func TagNoCluster() *Tag {
	return &Tag{
		Name:  clusterTagLabel,
		Value: noCluster,
	}
}

func TagAccount(accountName string) *Tag {
	return &Tag{
		Name:  accountTagLabel,
		Value: accountName,
	}
}

func TagStream(streamName string) *Tag {
	return &Tag{
		Name:  streamTagLabel,
		Value: streamName,
	}
}

func TagServerProfile() *Tag {
	return TagArtifactType(profileArtifactType)
}

func TagProfileName(profileType string) *Tag {
	return &Tag{
		Name:  profileNameTagLabel,
		Value: profileType,
	}
}

func TagSpecial(special string) *Tag {
	return &Tag{
		Name:  specialTagLabel,
		Value: special,
	}
}
