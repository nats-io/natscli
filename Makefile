.PHONY: build install

install: build
	cp dist/nats_darwin_arm64_v8.0/xnats /opt/homebrew/bin/xnats

build:
	goreleaser build --snapshot --clean --single-target
