# Audit archive Format Overview

Archive is a utility (part of the `audit` package) to capture and store large number of artifacts related to a NATS deployment.

For example, capture all 'monitor' endpoint for all servers in a given cluster. Or capture all details of all streams in a given account, or capture the streams state and configuration.

Primary design choices for archive:
 - Single file: easy to share over email, web, chat, etc.
 - Compressed: most artifacts have a lot of redundancy
 - Indexed: can be queried in a generic way
 - Default to JSON: easy for humans to grok, and for programs to process

An "archive" is a ZIP file that conforms to a specific schema convention.

The `archive` package provides `Reader` and `Writer` classes.

## Artifact tagging and manifest

Rather than relying on path convention and contrived splitting, each artifact is added to the archive with a unique set of tags.

For example, the capture of a server `VARZ` endpoint may be tagged with:
 - `cluster:awseast_bar`
 - `server:bar_1`
 - `type:server_varz`

A filename for is derived from it's tags, and may look something like:
```
capture/clusters/awseast_bar/bar_1/variables.json
```

In addition to capture artifacts, the archive contains a few special files.

The most important special file is `manifest.json` which stores tags for each file in the archive.
The manifest allows programmatic discovery of the archive content, and can be used to build indices at read time.

An example usage is: list all streams discovered under a given account `A`.
Rather than having to infer this information from iterating and parsing files paths, the manifest can be traversed filtering by `account:A` and `type:stream_info`, and all matching files in the archive can be processed.

## Archive organization

n.b. Do not rely on path parsing and path conventions, query using the manifest instead.

n.b.: If a server does not belong to a cluster, then `cluster_name` is `unclustered` for all the paths below.

## Server-specific artifacts


`${prefix}/clusters/${cluster_name}/${server_name}/${artifact_type}.json`

Example: snapshot of health endpoint

## Account artifacts

Each account artifact can appear multiple times as it is captured through different servers.

`${prefix}/accounts/${account_name}/servers/${cluster_name}__${server_name}/${artifact_type}.json"`

Example: account connections

## Stream artifacts

Each stream artifact can appear multiple times as it is captured through different replicas.

`${prefix}/accounts/${account_name}/streams/${stream_name}/replicas/${server_name}/${artifact_type}.json`

Example: stream details

## Server profiles

`${prefix}/profiles/${cluster_name}/${server_name}__${profile_name}.prof`

Example: memory profile

## Special files

In addition to artifacts, each archive contains a few special files.

## Capture metadata

`${prefix}/capture_metadata.json`

Contains information such as the date of capture, the username, the tool version, and more.

## Manifest

`${prefix}/manifest.json`

Contains a list of all artifacts and a list of tags associated with each one.

## Capture log

`${prefix}/capture.log`

A log file for the process that created the archive, in case it contains useful information about artifacts (and lack of thereof).
