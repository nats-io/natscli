package archive

import (
	"fmt"
)

type TagLabel string

type Tag struct {
	Name  TagLabel
	Value string
}

const (
	serverTagLabel  TagLabel = "server"
	clusterTagLabel TagLabel = "cluster"
	accountTagLabel TagLabel = "account"
	streamTagLabel  TagLabel = "stream"
	typeTagLabel    TagLabel = "artifact_type"
)

const (
	healtzArtifactType        string = "health"
	varzArtifactType          string = "variables"
	connzArtifactType         string = "connections"
	routezArtifactType        string = "routes"
	gatewayzArtifactType      string = "gateways"
	leafzArtifactType         string = "leafs"
	subzArtifactType          string = "subs"
	jszArtifactType           string = "jetstream"
	accountzArtifactType      string = "accounts"
	streamDetailsArtifactType string = "stream_details"
	manifestArtifactType      string = "manifest"
)

const (
	ManifestFileName string = "capture/manifest.json"
	NoCluster        string = "unclustered"
)

const rootPrefix = "capture/"
const separator = "__"
const captureLogName = rootPrefix + "capture.log"
const metadataName = rootPrefix + "capture_info.json"

// Special tag that result in a special file path
var specialFilesTagMap = map[Tag]string{
	*internalTagManifest(): rootPrefix + "manifest.json",
}

// Special tags that get composed and combined in the filename
var dimensionTagsNames = map[TagLabel]interface{}{
	accountTagLabel: nil,
	clusterTagLabel: nil,
	serverTagLabel:  nil,
	streamTagLabel:  nil,
	typeTagLabel:    nil,
}

func createFilenameFromTags(tags []*Tag) (string, error) {

	if len(tags) < 1 {
		return "", fmt.Errorf("at least one tag is required")
	} else if len(tags) == 1 {
		// Single tag provided, is it one that has a special handling?
		tag := tags[0]
		fileName, isSpecialTag := specialFilesTagMap[*tag]
		if isSpecialTag {
			// Short-circuit and return the matching filename
			return fileName, nil
		}
	}

	// "Dimension" tags are special:
	// - Can have at most one value
	// - They get combined to produce the file path
	dimensionTagsMap := make(map[TagLabel]*Tag, len(tags))

	// Capture non-dimension tags here (unused for now)
	otherTags := make([]*Tag, 0, len(tags))

	for _, tag := range tags {

		// The 'special' tags should not be mixed with the rest
		if _, present := specialFilesTagMap[*tag]; present {
			return "", fmt.Errorf("tag '%s' is special and should not be combined with other tags", tag.Name)
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
		// TODO for the moment, the gather command is the only user, and it is not custom tags.
		// If we ever open the archiving API beyond, we may need to address this.
		panic(fmt.Sprintf("Unhandled tags: %v", otherTags))
	}

	accountTag, hasAccountTag := dimensionTagsMap[accountTagLabel], dimensionTagsMap[accountTagLabel] != nil
	clusterTag, hasClusterTag := dimensionTagsMap[clusterTagLabel], dimensionTagsMap[clusterTagLabel] != nil
	serverTag, hasServerTag := dimensionTagsMap[serverTagLabel], dimensionTagsMap[serverTagLabel] != nil
	streamTag, hasStreamTag := dimensionTagsMap[streamTagLabel], dimensionTagsMap[streamTagLabel] != nil
	typeTag, hasTypeTag := dimensionTagsMap[typeTagLabel], dimensionTagsMap[typeTagLabel] != nil

	var name string

	if !hasTypeTag {
		return "", fmt.Errorf("missing required tag for artifact type")
	} else if !hasServerTag {
		return "", fmt.Errorf("missing required tag for source server")
	} else if hasStreamTag {
		// Stream artifact must have account and cluster tag
		if !hasClusterTag || !hasAccountTag {
			return "", fmt.Errorf("stream artifact is missing cluster or account tags")
		}

		name = fmt.Sprintf("accounts/%s/streams/%s/server_%s__%s", accountTag.Value, streamTag.Value, serverTag.Value, typeTag.Value)

	} else if hasAccountTag {
		// Account artifact (but not a stream)
		name = fmt.Sprintf("accounts/%s/server_%s__%s", accountTag.Value, serverTag.Value, typeTag.Value)

	} else if hasServerTag {
		// Server artifact

		clusterName := NoCluster
		if hasClusterTag {
			clusterName = clusterTag.Value
		}
		name = fmt.Sprintf("clusters/%s/server_%s__%s", clusterName, serverTag.Value, typeTag.Value)

	} else {
		// TODO may add more cases later, for now bomb if none of the above applies
		panic(fmt.Sprintf("Unhandled tags combination: %+v", dimensionTagsMap))
	}

	//TODO could set suffix based on type. For now, everything JSON.
	name = rootPrefix + name + ".json"

	return name, nil
}

func TagArtifactType(artifactType string) *Tag {
	return &Tag{
		Name:  typeTagLabel,
		Value: artifactType,
	}
}

func TagHealth() *Tag {
	return TagArtifactType(healtzArtifactType)
}

func TagServerVars() *Tag {
	return TagArtifactType(varzArtifactType)
}

func TagConnections() *Tag {
	return TagArtifactType(connzArtifactType)
}

func TagRoutes() *Tag {
	return TagArtifactType(routezArtifactType)
}

func TagGateways() *Tag {
	return TagArtifactType(gatewayzArtifactType)
}

func TagLeafs() *Tag {
	return TagArtifactType(leafzArtifactType)
}

func TagSubs() *Tag {
	return TagArtifactType(subzArtifactType)
}

func TagJetStream() *Tag {
	return TagArtifactType(jszArtifactType)
}

func TagAccounts() *Tag {
	return TagArtifactType(accountzArtifactType)
}

func TagStreamDetails() *Tag { return TagArtifactType(streamDetailsArtifactType) }

func internalTagManifest() *Tag {
	return TagArtifactType(manifestArtifactType)
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
		Value: NoCluster,
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
