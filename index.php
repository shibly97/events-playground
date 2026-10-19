<?php

declare(strict_types=1);

// Events playground: uses clicksend/clicksend exactly like a consuming service would,
// then reads the Kinesis stream to show whether each action really reached it.

require __DIR__ . '/vendor/autoload.php';

use Aws\Kinesis\KinesisClient;
use ClickSend\ClickSend\Events\EventContext;
use ClickSend\ClickSend\Events\EventFactory;
use ClickSend\ClickSend\Events\EventPublisher;
use ClickSend\ClickSend\Events\Events;
use ClickSend\ClickSend\Events\Time\Timestamp;
use ClickSend\EventSchemas\EventType;
use Psr\Log\AbstractLogger;

const SERVICE_NAME = 'service-sms-mo';

// ---------------------------------------------------------------- helpers

/** Captures everything the package logs (including metrics) so the page can show it. */
final class PageLogger extends AbstractLogger
{
    /** @var list<array{level: string, message: string, context: array<mixed>}> */
    public array $lines = [];

    public function log($level, $message, array $context = []): void
    {
        $this->lines[] = ['level' => (string) $level, 'message' => (string) $message, 'context' => $context];
    }
}

function envValue(string $name, string $default = ''): string
{
    $value = getenv($name);

    return $value === false ? $default : $value;
}

/** Overrides an env var for this request only, for the "what if" scenarios. */
function overrideEnv(string $name, string $value): void
{
    putenv($name . '=' . $value);
    $_ENV[$name] = $value;
    $_SERVER[$name] = $value;
}

function h(mixed $value): string
{
    return htmlspecialchars((string) $value, ENT_QUOTES);
}

/** @return array<string, mixed> */
function smsData(string $text = 'Hello from the playground'): array
{
    return [
        'supplierAccountId' => 42,
        'body' => [
            'messageType' => 'SMS',
            'to' => '+61400000001',
            'from' => '+61400000002',
            'sentTimestamp' => gmdate('Y-m-d\TH:i:s\Z'),
            'message' => ['text' => $text],
        ],
    ];
}

function context(): EventContext
{
    return new EventContext(accountId: '123456', userId: '789', subaccountId: '456');
}

function messageId(): string
{
    return 'msg-' . substr(bin2hex(random_bytes(4)), 0, 8);
}

// Captured before any scenario overrides it, so the page always reads the real stream.
define('READ_ENDPOINT', envValue('AWS_ENDPOINT_URL_KINESIS'));

// No endpoint override means a real AWS stream, which other services also write to.
define('REAL_AWS', READ_ENDPOINT === '');

function kinesisReader(): KinesisClient
{
    // Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, like the publisher.
    return new KinesisClient([
        'region' => envValue('EVENTS_KINESIS_REGION', 'ap-southeast-2'),
        'version' => '2013-12-02',
        'http' => ['connect_timeout' => 1, 'timeout' => 3],
        ...(READ_ENDPOINT !== '' ? ['endpoint' => READ_ENDPOINT] : []),
    ]);
}

/**
 * Reads every record currently in the stream.
 *
 * @return array{ok: bool, error?: string, records: list<array{seq: string, pk: string, shard: string, data: string}>}
 */
function readStream(string $stream): array
{
    try {
        $client = kinesisReader();
        $records = [];

        foreach ($client->listShards(['StreamName' => $stream])['Shards'] as $shard) {
            $iterator = $client->getShardIterator([
                'StreamName' => $stream,
                'ShardId' => $shard['ShardId'],
                // On a shared real stream, only look at the last 15 minutes instead of the whole retention.
                ...(REAL_AWS
                    ? ['ShardIteratorType' => 'AT_TIMESTAMP', 'Timestamp' => time() - 900]
                    : ['ShardIteratorType' => 'TRIM_HORIZON']),
            ])['ShardIterator'];

            for ($page = 0; $iterator !== null && $page < 50; $page++) {
                $result = $client->getRecords(['ShardIterator' => $iterator, 'Limit' => 1000]);

                foreach ($result['Records'] as $record) {
                    // Never show other services' events (they may hold real customer data).
                    if (REAL_AWS && (json_decode((string) $record['Data'], true)['source'] ?? null) !== 'urn:clicksend:' . SERVICE_NAME) {
                        continue;
                    }

                    $records[] = [
                        'seq' => (string) $record['SequenceNumber'],
                        'pk' => (string) $record['PartitionKey'],
                        'shard' => (string) $shard['ShardId'],
                        'data' => (string) $record['Data'],
                    ];
                }

                if ($result['Records'] === [] && (int) ($result['MillisBehindLatest'] ?? 0) === 0) {
                    break;
                }

                $iterator = $result['NextShardIterator'] ?? null;
            }
        }

        return ['ok' => true, 'records' => $records];
    } catch (Throwable $e) {
        return ['ok' => false, 'error' => $e->getMessage(), 'records' => []];
    }
}

// ---------------------------------------------------------------- scenarios

$scenarios = [
    'publish' => [
        'title' => 'Publish a valid SMS event',
        'expect' => 'Sent. 1 new record in Kinesis, partition key = message id.',
    ],
    'batch' => [
        'title' => 'Publish 3 events in one batch',
        'expect' => 'One PutRecords call. 3 new records in Kinesis.',
    ],
    'pii' => [
        'title' => 'Phone number as partition key',
        'expect' => 'Blocked by the PII check. Nothing reaches Kinesis.',
    ],
    'invalid' => [
        'title' => 'Missing required field (body)',
        'expect' => 'Blocked by validation. Nothing reaches Kinesis.',
    ],
    'too_large' => [
        'title' => 'Event over 1 MiB',
        'expect' => 'Skipped as too_large. Nothing reaches Kinesis.',
    ],
    'missing_stream' => [
        'title' => 'Stream does not exist',
        'expect' => 'Retried 3 times, failure logged + counted, page still works.',
    ],
    'unreachable' => [
        'title' => 'Kinesis unreachable',
        'expect' => 'Connection fails, failure logged + counted, page still works.',
    ],
    'disabled' => [
        'title' => 'Publishing disabled by env',
        'expect' => 'EVENTS_KINESIS_ENABLED=false: nothing happens, returns false.',
    ],
];

$stream = envValue('EVENTS_KINESIS_STREAM', 'events-playground');
$action = $_POST['action'] ?? null;
$outcome = null;

if ($action === 'create_stream' && !REAL_AWS) {
    try {
        kinesisReader()->createStream(['StreamName' => $stream, 'ShardCount' => 1]);
        kinesisReader()->waitUntil('StreamExists', ['StreamName' => $stream]);
        $outcome = ['title' => 'Create stream', 'note' => "Stream \"$stream\" created."];
    } catch (Aws\Kinesis\Exception\KinesisException $e) {
        $outcome = ['title' => 'Create stream', 'note' => $e->getAwsErrorCode() === 'ResourceInUseException'
            ? "Stream \"$stream\" already exists."
            : $e->getMessage()];
    } catch (Throwable $e) {
        $outcome = ['title' => 'Create stream', 'note' => $e->getMessage()];
    }
} elseif (is_string($action) && isset($scenarios[$action])) {
    // Per-scenario configuration changes, applied before the publisher reads the env.
    match ($action) {
        'missing_stream' => overrideEnv('EVENTS_KINESIS_STREAM', 'no-such-stream'),
        'unreachable' => overrideEnv('AWS_ENDPOINT_URL_KINESIS', 'http://127.0.0.1:1'),
        'disabled' => overrideEnv('EVENTS_KINESIS_ENABLED', 'false'),
        default => null,
    };

    $before = readStream($stream);
    $logger = new PageLogger();

    // ---- This is what a real service does ----
    Events::setPublisher(EventPublisher::fromEnv(SERVICE_NAME, $logger));

    $returned = null;
    $escaped = null;
    $startedAt = hrtime(true);

    try {
        $id = messageId();

        $returned = match ($action) {
            'batch' => Events::publishEvents(array_map(
                static fn (int $i) => (new EventFactory(SERVICE_NAME, new Timestamp()))->create(
                    EventType::MESSAGING_SMS_INBOUND_RECEIVED,
                    smsData("Batch message $i"),
                    "sms/$id-$i",
                    "$id-$i",
                    context(),
                ),
                [1, 2, 3],
            )),
            'pii' => Events::publish(EventType::MESSAGING_SMS_INBOUND_RECEIVED, smsData(), "sms/$id", '+61400000001', context()),
            'invalid' => Events::publish(
                EventType::MESSAGING_SMS_INBOUND_RECEIVED,
                array_diff_key(smsData(), ['body' => true]),
                "sms/$id",
                $id,
                context(),
            ),
            'too_large' => Events::publish(
                EventType::MESSAGING_SMS_INBOUND_RECEIVED,
                smsData(str_repeat('x', 1_100_000)),
                "sms/$id",
                $id,
                context(),
            ),
            default => Events::publish(EventType::MESSAGING_SMS_INBOUND_RECEIVED, smsData(), "sms/$id", $id, context()),
        };
    } catch (Throwable $e) {
        $escaped = $e::class . ': ' . $e->getMessage();
    }
    // ---- end of what a real service does ----

    $elapsedMs = (hrtime(true) - $startedAt) / 1e6;
    $after = readStream($stream);
    $seen = array_flip(array_column($before['records'], 'seq'));

    $outcome = [
        'title' => $scenarios[$action]['title'],
        'expect' => $scenarios[$action]['expect'],
        'returned' => $returned,
        'escaped' => $escaped,
        'elapsed' => $elapsedMs,
        'logs' => $logger->lines,
        'readable' => $after['ok'],
        'readError' => $after['error'] ?? null,
        'newRecords' => array_values(array_filter($after['records'], static fn (array $r) => !isset($seen[$r['seq']]))),
    ];
}

$current = readStream($stream);
$latest = array_slice(array_reverse($current['records']), 0, 10);

?><!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Events Playground</title>
<style>
    :root { --bg:#f6f7f9; --card:#fff; --ink:#1d2330; --muted:#667085; --line:#e4e7ec; --ok:#067647; --okbg:#ecfdf3; --bad:#b42318; --badbg:#fef3f2; --warn:#b54708; --warnbg:#fffaeb; --accent:#2e5aac; }
    * { box-sizing: border-box; }
    body { margin:0; font:14px/1.5 -apple-system, system-ui, sans-serif; background:var(--bg); color:var(--ink); }
    main { max-width:1100px; margin:0 auto; padding:24px 16px 64px; }
    h1 { margin:0 0 4px; font-size:22px; }
    h2 { font-size:16px; margin:28px 0 10px; }
    .muted { color:var(--muted); }
    .config { display:flex; flex-wrap:wrap; gap:8px; margin:12px 0 0; }
    .config code { background:var(--card); border:1px solid var(--line); border-radius:6px; padding:2px 8px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(240px, 1fr)); gap:12px; }
    form { margin:0; }
    button.scenario { width:100%; height:100%; text-align:left; background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; cursor:pointer; font:inherit; color:inherit; }
    button.scenario:hover { border-color:var(--accent); box-shadow:0 1px 4px rgba(0,0,0,.06); }
    button.scenario strong { display:block; margin-bottom:4px; }
    button.scenario span { color:var(--muted); font-size:13px; }
    button.secondary { background:var(--accent); color:#fff; border:0; border-radius:8px; padding:8px 14px; cursor:pointer; font:inherit; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; overflow:hidden; }
    .facts { display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:10px; margin:12px 0; }
    .fact { border-radius:8px; padding:10px 12px; background:var(--bg); }
    .fact b { display:block; font-size:12px; color:var(--muted); font-weight:500; }
    .ok { background:var(--okbg); color:var(--ok); }
    .bad { background:var(--badbg); color:var(--bad); }
    .warn { background:var(--warnbg); color:var(--warn); }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }
    pre { margin:0; white-space:pre-wrap; word-break:break-word; font-size:12px; }
    details summary { cursor:pointer; color:var(--accent); }
</style>
</head>
<body>
<main>
    <h1>Events Playground</h1>
    <div class="muted">Uses <code>clicksend/clicksend</code> installed with Composer, the same way a service does, and reads the stream back to prove what reached Kinesis.</div>
    <div class="config">
        <code>stream: <?= h($stream) ?></code>
        <code>enabled: <?= h(envValue('EVENTS_KINESIS_ENABLED', '(unset)')) ?></code>
        <code>region: <?= h(envValue('EVENTS_KINESIS_REGION', '(unset)')) ?></code>
        <code>endpoint: <?= h(envValue('AWS_ENDPOINT_URL_KINESIS', 'real AWS')) ?></code>
    </div>

    <?php if (!$current['ok']): ?>
        <div class="card warn" style="margin-top:16px">
            Can't read stream "<?= h($stream) ?>": <?= h($current['error'] ?? '') ?>
            <?php if (!REAL_AWS): ?>
                <form method="post" style="margin-top:8px"><button class="secondary" name="action" value="create_stream">Create stream</button></form>
            <?php endif; ?>
        </div>
    <?php endif; ?>

    <h2>Try a scenario</h2>
    <div class="grid">
        <?php foreach ($scenarios as $key => $scenario): ?>
            <form method="post">
                <button class="scenario" name="action" value="<?= h($key) ?>">
                    <strong><?= h($scenario['title']) ?></strong>
                    <span><?= h($scenario['expect']) ?></span>
                </button>
            </form>
        <?php endforeach; ?>
    </div>

    <?php if ($outcome !== null): ?>
        <h2>Result: <?= h($outcome['title']) ?></h2>
        <div class="card">
            <?php if (isset($outcome['note'])): ?>
                <?= h($outcome['note']) ?>
            <?php else: ?>
                <div class="muted">Expected: <?= h($outcome['expect']) ?></div>
                <div class="facts">
                    <div class="fact <?= $outcome['escaped'] === null ? 'ok' : 'bad' ?>">
                        <b>Exception reached your code?</b>
                        <?= $outcome['escaped'] === null ? 'No, business flow continued' : 'YES: ' . h($outcome['escaped']) ?>
                    </div>
                    <div class="fact">
                        <b>Return value</b>
                        <?= h(var_export($outcome['returned'], true)) ?>
                    </div>
                    <div class="fact">
                        <b>Time spent in publish</b>
                        <?= h(number_format($outcome['elapsed'], 1)) ?> ms
                    </div>
                    <div class="fact <?= count($outcome['newRecords']) > 0 ? 'ok' : 'warn' ?>">
                        <b>New records in Kinesis</b>
                        <?= $outcome['readable'] ? count($outcome['newRecords']) : 'stream not readable' ?>
                    </div>
                </div>

                <?php if ($outcome['newRecords'] !== []): ?>
                    <table>
                        <tr><th>Partition key</th><th>Shard</th><th>Event</th></tr>
                        <?php foreach ($outcome['newRecords'] as $record): ?>
                            <tr>
                                <td><code><?= h($record['pk']) ?></code></td>
                                <td><?= h($record['shard']) ?></td>
                                <td><details><summary>show JSON</summary><pre><?= h(json_encode(json_decode($record['data']), JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)) ?></pre></details></td>
                            </tr>
                        <?php endforeach; ?>
                    </table>
                <?php endif; ?>

                <h3 style="font-size:14px;margin:16px 0 6px">What the package logged</h3>
                <?php if ($outcome['logs'] === []): ?>
                    <div class="muted">Nothing.</div>
                <?php else: ?>
                    <table>
                        <tr><th>Level</th><th>Message</th><th>Context</th></tr>
                        <?php foreach ($outcome['logs'] as $line): ?>
                            <tr>
                                <td><?= h($line['level']) ?></td>
                                <td><?= h($line['message']) ?></td>
                                <td><pre><?= h(json_encode($line['context'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)) ?></pre></td>
                            </tr>
                        <?php endforeach; ?>
                    </table>
                <?php endif; ?>
            <?php endif; ?>
        </div>
    <?php endif; ?>

    <h2>Latest records in "<?= h($stream) ?>" (<?= count($current['records']) ?> total)</h2>
    <div class="card">
        <?php if ($latest === []): ?>
            <div class="muted">No records yet.</div>
        <?php else: ?>
            <table>
                <tr><th>Partition key</th><th>Type</th><th>Time</th><th>Event</th></tr>
                <?php foreach ($latest as $record): $event = json_decode($record['data'], true) ?? []; ?>
                    <tr>
                        <td><code><?= h($record['pk']) ?></code></td>
                        <td><?= h($event['type'] ?? '') ?></td>
                        <td><?= h($event['time'] ?? '') ?></td>
                        <td><details><summary>show JSON</summary><pre><?= h(json_encode($event, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)) ?></pre></details></td>
                    </tr>
                <?php endforeach; ?>
            </table>
        <?php endif; ?>
    </div>
</main>
</body>
</html>
