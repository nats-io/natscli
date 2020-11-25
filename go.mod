module github.com/nats-io/jetstream

go 1.14

replace github.com/codahale/hdrhistogram => github.com/HdrHistogram/hdrhistogram-go v0.9.0

require (
	github.com/AlecAivazis/survey/v2 v2.2.2
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/codahale/hdrhistogram v0.0.0-00010101000000-000000000000
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.10.0
	github.com/ghodss/yaml v1.0.0
	github.com/google/go-cmp v0.5.3
	github.com/gosuri/uilive v0.0.4 // indirect
	github.com/gosuri/uiprogress v0.0.1
	github.com/guptarohit/asciigraph v0.5.1
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/nats-io/jsm.go v0.0.20-0.20201125175533-fecd1b5dbb07
	github.com/nats-io/nats-server/v2 v2.1.8-0.20201125160748-de0c992ca6db
	github.com/nats-io/nats.go v1.10.1-0.20201111151633-9e1f4a0d80d8
	github.com/tylertreat/hdrhistogram-writer v0.0.0-20180430173243-73b8d31ba571
	github.com/xeipuuv/gojsonschema v1.2.0
	github.com/xlab/tablewriter v0.0.0-20160610135559-80b567a11ad5
	golang.org/x/crypto v0.0.0-20201016220609-9e8e0b390897
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/yaml.v2 v2.3.0 // indirect
)
