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
	healtzArtifactType   string = "health"
	varzArtifactType     string = "variables"
	connzArtifactType    string = "connections"
	routezArtifactType   string = "routes"
	gatewayzArtifactType string = "gateways"
	leafzArtifactType    string = "leafs"
	subzArtifactType     string = "subs"
	jszArtifactType      string = "jetstream"
	accountzArtifactType string = "accounts"
	manifestArtifactType string = "manifest"
)

const (
	ManifestFileName string = "manifest.json"
)

const fileNamePrefix = "artifact"
const separator = "__"

var specialFilesTagMap = map[Tag]string{
	*internalTagManifest(): "manifest.json",
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

	// Tags that get composed and combined in the filename
	dimensionTagsMap := map[TagLabel]*Tag{
		accountTagLabel: nil,
		clusterTagLabel: nil,
		serverTagLabel:  nil,
		streamTagLabel:  nil,
		typeTagLabel:    nil,
	}

	// Order in which 'dimension' tags appear in the name and format string
	dimensionTagsOrder := []struct {
		label          TagLabel
		filenameFormat string
	}{
		{accountTagLabel, "account_%s"},
		{clusterTagLabel, "cluster_%s"},
		{serverTagLabel, "server_%s"},
		{streamTagLabel, "stream_%s"},
		{typeTagLabel, "%s"},
	}

	for _, tag := range tags {

		// The 'special' tags should not be mixed with the rest
		if _, present := specialFilesTagMap[*tag]; present {
			return "", fmt.Errorf("tag '%s' is special and should not be combined with other tags", tag.Name)
		}

		existingTag, isDimensionTag := dimensionTagsMap[tag.Name]

		if isDimensionTag {
			// If it's a dimension tag, save it
			if existingTag != nil {
				// Dimension tags can only have at most one value per file
				return "", fmt.Errorf("duplicate tag %s", tag.Name)
			}
			dimensionTagsMap[tag.Name] = tag

		} else {
			// Store other (non-dimension) tags
			// TODO
		}
	}

	// TODO do some checking
	// Cannot be stream and consumer
	// Cannot be missing a type
	// Must have at least one dimension or at least one custom tag

	var name = fileNamePrefix
	for _, dimensionTagOption := range dimensionTagsOrder {
		dimensionTag := dimensionTagsMap[dimensionTagOption.label]
		if dimensionTag != nil {
			name = fmt.Sprintf("%s%s"+dimensionTagOption.filenameFormat, name, separator, dimensionTag.Value)
		}
	}

	//TODO could set suffix based on type. For now, all JSON.
	name = name + ".json"

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
