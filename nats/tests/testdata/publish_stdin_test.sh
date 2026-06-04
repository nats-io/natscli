#!/bin/sh

SERVER="$1"
FILE="$2"

if [ -z "$SERVER" ] || [ -z "$FILE" ]; then
  echo "Usage: $0 <server_url> <input_file>" >&2
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "Input file not found: $FILE" >&2
  exit 1
fi

# Choose CLI command based on environment
if [ "$CI" = "true" ]; then
  CMD="../nats"
else
  CMD="go run ../main.go"
fi

while IFS=';' read -r topic message; do
  if [ -n "$topic" ] && [ -n "$message" ]; then
    $CMD --server="$SERVER" pub "$topic" "$message"
  fi
done < "$FILE"
