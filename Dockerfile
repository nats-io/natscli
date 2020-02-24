FROM synadia/nats-server:2.2.0-JS-preview AS JS
FROM stedolan/jq:latest AS JQ
FROM synadia/nats-box:latest

COPY --from=JS /nats-server /nats-server
COPY --from=JQ /usr/local/bin/jq /usr/local/bin/jq

# goreleaser does the build
COPY nats /usr/local/bin/
COPY README.md /
COPY ngs-server.conf /
COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]

EXPOSE 4222
ENV NATS_URL=jetstream:4222

RUN apk add --update ca-certificates man bash && \
    mkdir -p /usr/share/man/man1 && \
    nats --help-man > /usr/share/man/man1/nats.1
