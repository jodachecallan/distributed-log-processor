#!/bin/sh
set -e

echo "Starting $SCRIPT with user=$USERNAME host=$HOST port=$PORT"

exec python "$SCRIPT" --username "$USERNAME" --password "$PASSWORD" --host "$HOST" --port "$PORT"
