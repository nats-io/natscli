FROM synadia/nats-box:latest

# goreleaser does the build
COPY jsm /usr/local/bin/

RUN apk add --update ca-certificates man && \
    mkdir -p /usr/share/man/man1 && \
    jsm --help-man > /usr/share/man/man1/jsm.1
