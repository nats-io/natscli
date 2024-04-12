nats.wasm: clean
	GOARCH=wasm GOOS=js go build -o nats.wasm
	cp nats.wasm ../../try.nats.io/public/nats.wasm

.PHONY: clean
clean:
	rm -f nats.wasm

