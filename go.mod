module github.com/nats-io/natscli

go 1.16

replace github.com/codahale/hdrhistogram => github.com/HdrHistogram/hdrhistogram-go v0.9.0

require (
	github.com/AlecAivazis/survey/v2 v2.2.12
	github.com/HdrHistogram/hdrhistogram-go v1.1.0
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/emicklei/dot v0.15.0
	github.com/fatih/color v1.12.0
	github.com/ghodss/yaml v1.0.0
	github.com/google/go-cmp v0.5.5
	github.com/gosuri/uilive v0.0.4 // indirect
	github.com/gosuri/uiprogress v0.0.1
	github.com/guptarohit/asciigraph v0.5.2
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/klauspost/compress v1.13.4
	github.com/nats-io/jsm.go v0.0.27-0.20210928130438-dae742089696
	github.com/nats-io/nats-server/v2 v2.6.1
	github.com/nats-io/nats.go v1.12.3
	github.com/nats-io/nuid v1.0.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.26.0
	github.com/tylertreat/hdrhistogram-writer v0.0.0-20210816161836-2e440612a39f
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	github.com/xlab/tablewriter v0.0.0-20160610135559-80b567a11ad5
	golang.org/x/crypto v0.0.0-20210616213533-5ff15b29337e
	golang.org/x/term v0.0.0-20210422114643-f5beecf764ed
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
