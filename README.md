# Events Playground

A small local web page for trying out the `clicksend/clicksend` events publisher by hand.
Each button runs one scenario and then reads the Kinesis stream back to show whether the
event really arrived.

It installs `clicksend/clicksend` with Composer, the same way a real service does, so it
also proves the package works outside its own repo. **Local use only. Not for production.**

## How it's wired

| Piece | Where it comes from |
|---|---|
| `clicksend/clicksend` | `../ClickSend` (symlinked, so it always uses whatever branch is checked out there) |
| `clicksend/event-schemas` | `../clicksend-event-schemas` (symlinked, pinned as `0.0.15`) |
| Kinesis | LocalStack, a fake AWS running in Docker on `localhost:4566` |
| Config | Environment variables set in `start.sh`, the same ones a real service sets |

Both packages come from local folders because they're private. Once you have a Private
Packagist token, you can replace the two `path` repositories in `composer.json` with
`{ "type": "composer", "url": "https://repo.packagist.com/clicksend/" }`.

## Run it

Needs PHP 8.4, Composer and Docker.

```bash
brew install composer          # once
composer install               # once, or after changing composer.json
./start.sh                     # starts LocalStack + the page
```

Open http://localhost:8000. The first time, click **Create stream**. LocalStack forgets
streams when its container restarts.

Stop with Ctrl+C. LocalStack keeps running in the background; stop it with
`docker stop localstack`.

## Scenarios

| Button | Expected |
|---|---|
| Publish a valid SMS event | `true`, 1 new record, partition key = message id |
| Publish 3 events in one batch | `3`, 3 new records from one PutRecords call |
| Phone number as partition key | `false`, blocked by the PII check (`reason: pii`) |
| Missing required field (body) | `false`, blocked by validation (`reason: validation`) |
| Event over 1 MiB | `false`, skipped (`reason: too_large`) |
| Stream does not exist | `false`, retried 3 times, failure logged + counted |
| Kinesis unreachable | `false`, connection fails, failure logged + counted |
| Publishing disabled by env | `false`, nothing happens |

Every scenario must show **"Exception reached your code? No"**. That's the guarantee
that publishing never breaks the caller's business flow.

A *partial* batch failure can't be triggered with LocalStack. It's covered by the unit
test `testOnlyFailedRecordsAreRetried` in the ClickSend repo.

## AWS credentials

Credentials are never stored in the package or in this app. The AWS SDK finds them
itself, in this order:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
2. A profile in `~/.aws/credentials` / `~/.aws/config`, selected with `AWS_PROFILE`
3. The IAM role of the container or server (ECS task role, EC2 instance profile), which is what production uses

`start.sh` sets fake `test` / `test` keys because LocalStack accepts anything.

## Real dev stream

```bash
cp .env.example .env           # once, then set your AWS profile and stream name
bash start-aws.sh
```

This skips LocalStack and publishes to the stream named in `.env`, using that AWS profile's
credentials. `.env` is git-ignored, so account details stay on your machine.

The stream is shared, so in this mode the page:

- reads only the last 15 minutes, and only shows events whose `source` is
  `urn:clicksend:service-sms-mo`, so you never see other services' (possibly real) data
- hides **Create stream**

Your IAM user needs `kinesis:PutRecords` on the stream, plus `ListShards`,
`GetShardIterator` and `GetRecords` for the page to read it back.
