#!/usr/bin/env bash
# Starts the playground against a REAL Kinesis stream (no LocalStack).
# Events you publish land on a shared stream that other dev consumers may read.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
    echo 'Missing .env. Copy .env.example to .env and fill it in.' >&2
    exit 1
fi

# Load AWS_PROFILE, EVENTS_KINESIS_STREAM and EVENTS_KINESIS_REGION from .env.
set -a
source .env
set +a

# Use the real AWS endpoint and the profile's credentials, not LocalStack's fake ones.
unset AWS_ENDPOINT_URL_KINESIS AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

export EVENTS_KINESIS_ENABLED=true

echo "Profile: $AWS_PROFILE   Stream: $EVENTS_KINESIS_STREAM ($EVENTS_KINESIS_REGION)"
echo 'Open http://localhost:8000  (Ctrl+C to stop)'
php -S localhost:8000 index.php
