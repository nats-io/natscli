FROM synadia/nats-server:2.2.0-JS-preview AS JS

FROM synadia/nats-box:latest

COPY --from=JS /nats-server /nats-server

# goreleaser does the build
COPY jsm /usr/local/bin/

RUN apk add --update ca-certificates man && \
    mkdir -p /usr/share/man/man1 && \
    jsm --help-man > /usr/share/man/man1/jsm.1
