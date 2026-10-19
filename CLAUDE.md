# Context for Claude

This folder is a throwaway local playground, not a product. Read README.md first.

- Purpose: manually test the Kinesis events publisher in `../ClickSend` (package `clicksend/clicksend`)
  by installing it with Composer like a real consuming service.
- The publisher work lives in `../ClickSend` on branch `CDX-759`
  (uncommitted as of 2026-10-19): `src/Events/{Events,EventPublisher}.php`, `src/Events/Kinesis/`,
  `src/Events/Validation/`, `src/Events/Schema/`, `src/Events/Metrics/`, tests in `tests/Unit`, `tests/Feature`, `tests/Fixtures`.
- Pipeline: `Events::publish()` -> EventFactory -> EventValidator -> PiiChecker -> PublishKinesis (PutRecords,
  retries only failed records, never throws; failures logged + counted).
- Config is env only: EVENTS_KINESIS_ENABLED, EVENTS_KINESIS_STREAM, EVENTS_KINESIS_REGION, EVENTS_KINESIS_MAX_ATTEMPTS,
  EVENTS_KINESIS_MAX_RECORD_BYTES (default 1 MiB), EVENTS_KINESIS_MAX_REQUEST_BYTES (default 5 MiB, batches split to fit).
  The AWS SDK reads AWS_ENDPOINT_URL_KINESIS, which is how this playground points at LocalStack.
- `clicksend/event-schemas` is pinned to 0.0.15. Remaining schema mismatches (TODO in EventValidator):
  `accountid` not allowed by the schema; `traceparent` "" sent by the factory but schema needs minLength 1.
- Size check (asked for by the lead) is done: events over max record bytes are skipped (`too_large`), and
  PutRecords batches are split at 500 records or max request bytes. Lead said "5 MiB"; still to confirm whether
  that means per request (current default) or per event (needs the stream's MaxRecordSize raised too).
- The user is new to PHP; explain PHP/Composer/Laravel concepts plainly.
