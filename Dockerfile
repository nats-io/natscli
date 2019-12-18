FROM golang:1.13.4-alpine3.10 AS builder

WORKDIR $GOPATH/src/github.com/nats-io/jetstream

RUN apk add -U --no-cache git binutils

WORKDIR $GOPATH/src/github.com/nats-io/jetstream

COPY . .

RUN go install ./...
RUN strip /go/bin/*

FROM synadia/nats-box:latest

RUN apk add --update ca-certificates

COPY --from=builder /go/bin/* /usr/local/bin/

ENTRYPOINT ["/bin/sh", "-l"]
