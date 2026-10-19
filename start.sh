#!/usr/bin/env bash
# Starts LocalStack (fake Kinesis) and the playground web page.
set -euo pipefail
cd "$(dirname "$0")"

if ! docker ps --format '{{.Names}}' | grep -qx localstack; then
    docker start localstack >/dev/null 2>&1 \
        || docker run -d --name localstack -p 4566:4566 -e SERVICES=kinesis localstack/localstack:3.8 >/dev/null
fi

printf 'Waiting for LocalStack'
for _ in $(seq 1 60); do
    curl -s localhost:4566/_localstack/health | grep -q '"kinesis": "\(available\|running\)"' && break
    printf '.'; sleep 2
done
echo ' ready'

# Configuration exactly as a real service would set it.
export EVENTS_KINESIS_ENABLED=true
export EVENTS_KINESIS_STREAM=events-playground
export EVENTS_KINESIS_REGION=ap-southeast-2

# Point the AWS SDK at LocalStack instead of real AWS.
export AWS_ENDPOINT_URL_KINESIS=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo 'Open http://localhost:8000  (Ctrl+C to stop)'
php -S localhost:8000 index.php
