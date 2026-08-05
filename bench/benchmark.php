#!/usr/bin/env php
<?php
declare(strict_types=1);

use ChannelPhp\Channel;
use ChannelPhp\AsyncResult;
use ChannelPhp\Internal\JsonCodec;
use ChannelPhp\ClientSender;
use ChannelPhp\Sender;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Channel as CoChannel;
use OpenSwoole\WebSocket\Server as WebSocketServer;

require dirname(__DIR__) . '/vendor/autoload.php';

const DEFAULT_STAGE_MS = 5000;
const DEFAULT_RPS_PER_CLIENT = 1000;
const STAGE_CLIENTS = [1, 10, 100, 1000];

/**
 * @param list<int> $stageClients
 * @return array{language: string, results: list<array{rps: float, clients: int, client: array{cpu: float, mem: float}, server: array{cpu: float, mem: float}}>}
 */
function runOrchestrator(int $stageMs, int $rpsPerClient, array $stageClients): array
{
    $results = [];

    foreach ($stageClients as $clients) {
        $results[] = runStage($clients, $stageMs, $rpsPerClient);
    }

    return [
        'language' => 'php',
        'results' => $results,
    ];
}

/**
 * @return array{rps: float, clients: int, client: array{cpu: float, mem: float}, server: array{cpu: float, mem: float}}
 */
function runStage(int $clients, int $stageMs, int $rpsPerClient): array
{
    $port = allocatePort();

    $server = spawnWorker([
        'BENCH_MODE' => 'server',
        'BENCH_PORT' => (string) $port,
    ]);

    $client = spawnWorker([
        'BENCH_MODE' => 'client',
        'BENCH_HOST' => '127.0.0.1',
        'BENCH_PORT' => (string) $port,
        'BENCH_CLIENTS' => (string) $clients,
        'BENCH_RPS_PER_CLIENT' => (string) $rpsPerClient,
    ]);

    try {
        waitForReadyPair($server, $client, 15_000);

        usleep($stageMs * 1000);

        requireWorkerRunning($client, 'client');
        requireWorkerRunning($server, 'server');

        $clientUsage = sampleProcessUsage($client['pid']);
        $serverUsage = sampleProcessUsage($server['pid']);

        terminateWorker($client);
        terminateWorker($server);

        $clientResult = waitForMessage($client, 'result', 15_000);
        waitForMessage($server, 'result', 15_000);

        $responsesReceived = (float) ($clientResult['responsesReceived'] ?? 0.0);
        $rps = round($responsesReceived / ($stageMs / 1000), 2);

        waitForExit($client, 15_000);
        waitForExit($server, 15_000);

        return [
            'rps' => $rps,
            'clients' => $clients,
            'client' => $clientUsage,
            'server' => $serverUsage,
        ];
    } finally {
        terminateWorker($client);
        terminateWorker($server);
        waitForExit($client, 2_000);
        waitForExit($server, 2_000);
        closeWorker($client);
        closeWorker($server);
    }
}

/**
 * @return array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * }
 */
function spawnWorker(array $env): array
{
    $descriptor = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    $cmd = [PHP_BINARY, __FILE__];
    $process = proc_open($cmd, $descriptor, $pipes, dirname(__DIR__), array_merge($_ENV, getenvAsArray(), $env));
    if (!is_resource($process)) {
        throw new RuntimeException('Failed to spawn worker process.');
    }

    foreach ($pipes as $pipe) {
        stream_set_blocking($pipe, false);
    }

    $status = proc_get_status($process);
    $pid = (int) ($status['pid'] ?? 0);
    if ($pid <= 0) {
        throw new RuntimeException('Worker started without a valid PID.');
    }

    return [
        'process' => $process,
        'pipes' => $pipes,
        'pid' => $pid,
        'stdout_buffer' => '',
        'stdout_messages' => [],
    ];
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 */
function waitForReadyPair(array &$server, array &$client, int $timeoutMs): void
{
    $deadline = hrtime(true) + ($timeoutMs * 1_000_000);
    $serverReady = false;
    $clientReady = false;

    while (hrtime(true) < $deadline) {
        if (!$serverReady) {
            $message = popMessageByType($server, 'ready');
            if ($message !== null) {
                updateWorkerPidFromMessage($server, $message);
                $serverReady = true;
            }
        }

        if (!$clientReady) {
            $message = popMessageByType($client, 'ready');
            if ($message !== null) {
                updateWorkerPidFromMessage($client, $message);
                $clientReady = true;
            }
        }

        if ($serverReady && $clientReady) {
            return;
        }

        usleep(5_000);
    }

    throw new RuntimeException('Timed out waiting for ready messages from benchmark workers.');
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 * @param array<string,mixed> $message
 */
function updateWorkerPidFromMessage(array &$worker, array $message): void
{
    $pid = $message['pid'] ?? null;
    if (is_int($pid) && $pid > 0) {
        $worker['pid'] = $pid;
    }
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 * @return array<string,mixed>
 */
function waitForMessage(array &$worker, string $type, int $timeoutMs): array
{
    $deadline = hrtime(true) + ($timeoutMs * 1_000_000);

    while (hrtime(true) < $deadline) {
        $message = popMessageByType($worker, $type);
        if ($message !== null) {
            return $message;
        }

        usleep(5_000);
    }

    throw new RuntimeException(sprintf('Timed out waiting for worker message type "%s".', $type));
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 * @return array<string,mixed>|null
 */
function popMessageByType(array &$worker, string $type): ?array
{
    drainWorkerPipes($worker);

    foreach ($worker['stdout_messages'] as $index => $message) {
        if (($message['type'] ?? null) === 'error') {
            $text = (string) ($message['message'] ?? 'unknown worker error');
            throw new RuntimeException($text);
        }

        if (($message['type'] ?? null) === $type) {
            array_splice($worker['stdout_messages'], $index, 1);
            return $message;
        }
    }

    return null;
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 */
function drainWorkerPipes(array &$worker): void
{
    $stdout = stream_get_contents($worker['pipes'][1]);
    if (is_string($stdout) && $stdout !== '') {
        $worker['stdout_buffer'] .= $stdout;
        while (true) {
            $index = strpos($worker['stdout_buffer'], "\n");
            if ($index === false) {
                break;
            }

            $line = substr($worker['stdout_buffer'], 0, $index);
            $worker['stdout_buffer'] = (string) substr($worker['stdout_buffer'], $index + 1);
            $message = decodeJsonLine($line);
            if ($message !== null) {
                $worker['stdout_messages'][] = $message;
            }
        }
    }

    $stderr = stream_get_contents($worker['pipes'][2]);
    if (is_string($stderr) && trim($stderr) !== '') {
        fwrite(STDERR, $stderr);
    }
}

/**
 * @return array{cpu: float, mem: float}
 */
function sampleProcessUsage(int $pid): array
{
    $descriptor = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    $cmd = ['ps', '-p', (string) $pid, '-o', '%cpu=', '-o', 'rss='];
    $proc = proc_open($cmd, $descriptor, $pipes, dirname(__DIR__));
    if (!is_resource($proc)) {
        throw new RuntimeException('Failed to launch ps for usage sampling.');
    }

    fclose($pipes[0]);
    $stdout = (string) stream_get_contents($pipes[1]);
    $stderr = (string) stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $exit = proc_close($proc);

    if ($exit !== 0) {
        throw new RuntimeException(sprintf('Failed to sample process usage for pid %d: %s', $pid, trim($stderr)));
    }

    $parts = preg_split('/\s+/', trim($stdout)) ?: [];
    if (count($parts) < 2 || !is_numeric($parts[0]) || !is_numeric($parts[1])) {
        throw new RuntimeException(sprintf('Unexpected ps output for pid %d: "%s"', $pid, trim($stdout)));
    }

    $cpu = (float) ($parts[0] ?? 0.0);
    $rssKb = (float) ($parts[1] ?? 0.0);

    return [
        'cpu' => round($cpu, 2),
        'mem' => round($rssKb / 1024, 2),
    ];
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 */
function requireWorkerRunning(array &$worker, string $name): void
{
    drainWorkerPipes($worker);
    $status = proc_get_status($worker['process']);
    if (!($status['running'] ?? false)) {
        throw new RuntimeException(sprintf('Benchmark %s worker exited before sampling.', $name));
    }
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 */
function terminateWorker(array &$worker): void
{
    if (!is_resource($worker['process'])) {
        return;
    }

    $status = proc_get_status($worker['process']);
    if (!($status['running'] ?? false)) {
        return;
    }

    proc_terminate($worker['process'], SIGTERM);
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 */
function waitForExit(array &$worker, int $timeoutMs): void
{
    $deadline = hrtime(true) + ($timeoutMs * 1_000_000);
    while (hrtime(true) < $deadline) {
        if (!is_resource($worker['process'])) {
            return;
        }

        $status = proc_get_status($worker['process']);
        if (!($status['running'] ?? false)) {
            return;
        }

        drainWorkerPipes($worker);
        usleep(5_000);
    }
}

/**
 * @param array{
 *   process: resource,
 *   pipes: array{0: resource,1: resource,2: resource},
 *   pid: int,
 *   stdout_buffer: string,
 *   stdout_messages: list<array<string,mixed>>
 * } $worker
 */
function closeWorker(array &$worker): void
{
    if (!is_resource($worker['process'])) {
        return;
    }

    foreach ($worker['pipes'] as $pipe) {
        if (is_resource($pipe)) {
            fclose($pipe);
        }
    }

    proc_close($worker['process']);
}

function runServerWorker(): int
{
    if (!class_exists(WebSocketServer::class)) {
        emitJson('error', ['message' => 'ext-openswoole websocket server is required']);
        return 1;
    }

    $port = getenvInt('BENCH_PORT');
    if ($port <= 0) {
        emitJson('error', ['message' => 'BENCH_PORT must be a valid positive integer']);
        return 1;
    }

    $startedAt = hrtime(true);
    $runningServer = null;
    $stopRequested = false;

    if (function_exists('pcntl_signal')) {
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, static function () use (&$stopRequested): void {
            $stopRequested = true;
        });
        pcntl_signal(SIGINT, static function () use (&$stopRequested): void {
            $stopRequested = true;
        });
    }

    try {
        $runningServer = (new Channel())
            ->post('text/count', static function (array $request): int {
                $text = $request['body']['text'] ?? null;
                return is_string($text) ? strlen($text) : 0;
            })
            ->listen($port);
    } catch (Throwable $error) {
        emitJson('error', ['message' => $error->getMessage()]);
        return 1;
    }

    emitJson('ready', [
        'pid' => $runningServer->pid(),
        'port' => $runningServer->port,
    ]);

    while (!$stopRequested) {
        usleep(10_000);
    }

    try {
        $runningServer->stop();
    } catch (Throwable) {
    }

    $elapsedSeconds = (hrtime(true) - $startedAt) / 1_000_000_000;
    emitJson('result', [
        'pid' => $runningServer->pid(),
        'elapsedSeconds' => $elapsedSeconds,
    ]);

    return 0;
}

/**
 * @return array{0:int,1:int,2:float}
 */
function runProcessScenario(Sender $sender, string $path, int $durationMs, bool &$stopRequested): array
{
    $completed = 0;
    $errors = 0;
    $startedAt = hrtime(true);
    $deadline = $startedAt + ($durationMs * 1_000_000);

    while (!$stopRequested && hrtime(true) < $deadline) {
        try {
            $sender->send($path);
            $completed++;
        } catch (Throwable) {
            $errors++;
        }
    }

    $elapsedSeconds = (hrtime(true) - $startedAt) / 1_000_000_000;
    return [$completed, $errors, $elapsedSeconds];
}

function runProcessWorker(): int
{
    $totalRequestsRaw = getenv('BENCH_TOTAL_REQUESTS');
    $totalRequests = null;
    if (is_string($totalRequestsRaw) && $totalRequestsRaw !== '') {
        if (!preg_match('/^\\d+$/', $totalRequestsRaw)) {
            emitJson('error', ['message' => 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer']);
            return 1;
        }
        $totalRequests = (int) $totalRequestsRaw;
    }

    $durationMs = $totalRequests === null ? getenvInt('BENCH_DURATION_MS') : 0;
    if ($totalRequests === null && $durationMs <= 0) {
        emitJson('error', ['message' => 'BENCH_DURATION_MS must be a valid positive integer']);
        return 1;
    }

    $stopRequested = false;
    if (function_exists('pcntl_signal')) {
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, static function () use (&$stopRequested): void {
            $stopRequested = true;
        });
        pcntl_signal(SIGINT, static function () use (&$stopRequested): void {
            $stopRequested = true;
        });
    }

    $sender = (new Channel())
        ->post('sync', static fn(array $request): int => 1)
        ->post('async', static fn(array $request): AsyncResult => AsyncResult::resolved(1))
        ->process();

    emitJson('ready', ['pid' => getmypid()]);

    if ($totalRequests !== null) {
        $completed = 0;
        $errors = 0;
        $startedAt = hrtime(true);

        while (!$stopRequested && ($completed + $errors) < $totalRequests) {
            try {
                $sender->send('sync');
                $completed++;
            } catch (Throwable) {
                $errors++;
            }
        }

        $sender->stop();

        emitJson('result', [
            'pid' => getmypid(),
            'completed' => $completed,
            'errors' => $errors,
            'elapsedSeconds' => (hrtime(true) - $startedAt) / 1_000_000_000,
        ]);
        return 0;
    }

    [$syncCompleted, $syncErrors, $syncElapsedSeconds] =
        runProcessScenario($sender, 'sync', $durationMs, $stopRequested);
    [$asyncCompleted, $asyncErrors, $asyncElapsedSeconds] =
        runProcessScenario($sender, 'async', $durationMs, $stopRequested);

    $sender->stop();

    emitJson('result', [
        'pid' => getmypid(),
        'syncCompleted' => $syncCompleted,
        'syncErrors' => $syncErrors,
        'syncElapsedSeconds' => $syncElapsedSeconds,
        'asyncCompleted' => $asyncCompleted,
        'asyncErrors' => $asyncErrors,
        'asyncElapsedSeconds' => $asyncElapsedSeconds,
        'asyncFallback' => false,
    ]);
    return 0;
}

function runClientWorker(): int
{
    if (!class_exists(Coroutine::class) || !function_exists('go')) {
        emitJson('error', ['message' => 'ext-openswoole coroutine support is required']);
        return 1;
    }

    $port = getenvInt('BENCH_PORT');
    $clientsCount = getenvInt('BENCH_CLIENTS');
    $totalRequestsRaw = getenv('BENCH_TOTAL_REQUESTS');
    $totalRequests = null;
    if (is_string($totalRequestsRaw) && $totalRequestsRaw !== '') {
        if (!preg_match('/^\d+$/', $totalRequestsRaw)) {
            emitJson('error', ['message' => 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer']);
            return 1;
        }
        $totalRequests = (int) $totalRequestsRaw;
    }

    $requestMode = strtolower(getenvString('BENCH_REQUEST_MODE', 'burst'));
    if ($requestMode !== 'burst' && $requestMode !== 'stream') {
        emitJson('error', ['message' => 'BENCH_REQUEST_MODE must be either burst or stream']);
        return 1;
    }

    $maxInFlight = getenvInt('BENCH_MAX_IN_FLIGHT');
    if ($maxInFlight <= 0) {
        $maxInFlight = 4096;
    }

    $rpsPerClient = $totalRequests === null ? getenvInt('BENCH_RPS_PER_CLIENT') : 0;
    $host = getenvString('BENCH_HOST', '127.0.0.1');

    if ($port <= 0) {
        emitJson('error', ['message' => 'BENCH_PORT must be a valid positive integer']);
        return 1;
    }
    if ($clientsCount <= 0) {
        emitJson('error', ['message' => 'BENCH_CLIENTS must be a valid positive integer']);
        return 1;
    }
    if ($totalRequests === null && $rpsPerClient <= 0) {
        emitJson('error', ['message' => 'BENCH_RPS_PER_CLIENT must be a positive number']);
        return 1;
    }

    $address = sprintf('ws://%s:%d', $host, $port);
    $startedAt = hrtime(true);
    $targetRps = $clientsCount * $rpsPerClient;
    $connectedCount = 0;
    $nextClientIndex = 0;
    $requestsSent = 0;
    $responsesReceived = 0;
    $errors = 0;
    $inFlight = 0;
    $running = false;
    $stopped = false;
    $failed = false;
    $stopRequested = false;
    $tickerStarted = false;
    $connected = array_fill(0, $clientsCount, false);
    $clients = [];

    pcntl_async_signals(true);
    pcntl_signal(SIGTERM, static function () use (&$stopRequested): void {
        $stopRequested = true;
    });
    pcntl_signal(SIGINT, static function () use (&$stopRequested): void {
        $stopRequested = true;
    });

    $previousErrorHandler = set_error_handler(static function (int $severity, string $message): bool {
        if (str_contains($message, 'OpenSwoole\\Coroutine\\Http\\Client::push(): websocket handshake failed')) {
            return true;
        }
        return false;
    });

    Coroutine::run(static function () use (
        $clientsCount,
        $address,
        &$clients,
        &$connected,
        &$connectedCount,
        &$running,
        &$tickerStarted,
        &$nextClientIndex,
        $targetRps,
        $totalRequests,
        $requestMode,
        $maxInFlight,
        &$requestsSent,
        &$responsesReceived,
        &$errors,
        &$inFlight,
        &$stopped,
        &$stopRequested,
        &$failed,
        $startedAt,
    ): void {
        $channel = new Channel();
        $setupDeadline = hrtime(true) + 10_000_000_000;
        $completionQueueSize = 200_000;
        $completionQueue = new CoChannel($completionQueueSize);
        $workers = max(32, min(1024, $clientsCount * 2));
        $completionWorkersRunning = true;

        for ($w = 0; $w < $workers; $w++) {
            go(static function () use (&$completionWorkersRunning, $completionQueue, &$responsesReceived, &$errors, &$inFlight): void {
                while ($completionWorkersRunning || $inFlight > 0) {
                    $item = $completionQueue->pop(0.05);
                    if (!$item instanceof ClientSender) {
                        continue;
                    }

                    try {
                        $item->send('text/count', ['text' => 'hello']);
                        $responsesReceived++;
                    } catch (Throwable) {
                        $errors++;
                    } finally {
                        $inFlight--;
                    }
                }
            });
        }

        for ($i = 0; $i < $clientsCount; $i++) {
            $index = $i;
            $clients[] = $channel->connect($address, [
                'onConnect' => static function () use (&$connected, &$connectedCount, $index): void {
                    if ($connected[$index]) {
                        return;
                    }
                    $connected[$index] = true;
                    $connectedCount++;
                },
            ]);
        }

        $startTicker = static function () use (
            &$running,
            &$tickerStarted,
            &$clients,
            &$nextClientIndex,
            $targetRps,
            $totalRequests,
            $requestMode,
            $maxInFlight,
            &$requestsSent,
            &$responsesReceived,
            &$errors,
            &$inFlight,
            &$stopRequested,
            $completionQueue,
            $clientsCount,
        ): void {
            if ($tickerStarted) {
                return;
            }
            $tickerStarted = true;
            $running = true;

            emitJson('ready', ['pid' => getmypid(), 'clients' => $clientsCount]);

            go(static function () use (
                &$running,
                &$clients,
                &$nextClientIndex,
                $targetRps,
                $totalRequests,
                $requestMode,
                $maxInFlight,
                &$requestsSent,
                &$responsesReceived,
                &$errors,
                &$inFlight,
                &$stopRequested,
                $completionQueue
            ): void {
                $tickMs = 10;
                $carry = 0.0;

                while ($running) {
                    if ($totalRequests !== null) {
                        if ($requestMode === 'stream') {
                            while ($requestsSent < $totalRequests && $inFlight < $maxInFlight) {
                                $sender = $clients[$nextClientIndex];
                                $nextClientIndex = ($nextClientIndex + 1) % count($clients);
                                $requestsSent++;
                                $inFlight++;
                                $queued = $completionQueue->push($sender, 0.0);
                                if (!$queued) {
                                    $errors++;
                                    $inFlight--;
                                }
                            }
                        } else {
                            $budget = 4096;
                            while ($requestsSent < $totalRequests && $budget > 0) {
                                $sender = $clients[$nextClientIndex];
                                $nextClientIndex = ($nextClientIndex + 1) % count($clients);
                                $requestsSent++;
                                $inFlight++;
                                $queued = $completionQueue->push($sender, 0.0);
                                if (!$queued) {
                                    $errors++;
                                    $inFlight--;
                                }
                                $budget--;
                            }
                        }

                        if ($requestsSent >= $totalRequests && ($responsesReceived + $errors) >= $totalRequests && $inFlight <= 0) {
                            $stopRequested = true;
                            break;
                        }
                    } else {
                        $carry += ($targetRps * $tickMs) / 1000.0;
                        $sends = (int) floor($carry);
                        $carry -= $sends;

                        for ($i = 0; $i < $sends; $i++) {
                            $sender = $clients[$nextClientIndex];
                            $nextClientIndex = ($nextClientIndex + 1) % count($clients);
                            $requestsSent++;
                            $inFlight++;

                            $queued = $completionQueue->push($sender, 0.0);
                            if (!$queued) {
                                // Keep scheduler non-blocking and record a failed dispatch when queue is saturated.
                                $errors++;
                                $inFlight--;
                            }
                        }
                    }

                    Coroutine::usleep($totalRequests !== null ? 1000 : $tickMs * 1000);
                }
            });
        };

        while (!$stopRequested) {
            if ($connectedCount === $clientsCount) {
                $startTicker();
            } elseif (hrtime(true) >= $setupDeadline) {
                emitJson('error', [
                    'message' => sprintf(
                        'Timed out waiting for clients to connect (%d/%d)',
                        $connectedCount,
                        $clientsCount,
                    ),
                ]);
                $failed = true;
                $stopRequested = true;
                break;
            }

            Coroutine::usleep(1_000);
        }

        if ($stopped) {
            return;
        }
        $stopped = true;
        $running = false;

        if ($failed) {
            foreach ($clients as $client) {
                $client->stop();
            }
            $completionWorkersRunning = false;
            return;
        }

        $waitUntil = hrtime(true) + 2_000_000_000;
        while ($inFlight > 0 && hrtime(true) < $waitUntil) {
            Coroutine::usleep(5_000);
        }

        foreach ($clients as $client) {
            $client->stop();
        }
        $completionWorkersRunning = false;

        $elapsedSeconds = (hrtime(true) - $startedAt) / 1_000_000_000;
        emitJson('result', [
            'pid' => getmypid(),
            'requestsSent' => $requestsSent,
            'responsesReceived' => $responsesReceived,
            'errors' => $errors,
            'elapsedSeconds' => $elapsedSeconds,
        ]);
    });

    if ($previousErrorHandler !== null) {
        set_error_handler($previousErrorHandler);
    } else {
        restore_error_handler();
    }

    return $failed ? 1 : 0;
}

function emitJson(string $type, array $payload): void
{
    $line = json_encode(array_merge(['type' => $type], $payload), JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
    fwrite(STDOUT, $line . PHP_EOL);
    fflush(STDOUT);
}

/**
 * @return array<string,mixed>|null
 */
function decodeJsonLine(string $line): ?array
{
    $trimmed = trim($line);
    if ($trimmed === '') {
        return null;
    }

    try {
        $decoded = json_decode($trimmed, true, 512, JSON_THROW_ON_ERROR);
    } catch (Throwable) {
        return null;
    }

    if (!is_array($decoded)) {
        return null;
    }

    return $decoded;
}

function allocatePort(): int
{
    $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if (!is_resource($server)) {
        throw new RuntimeException('Unable to allocate random port: ' . $errstr);
    }

    $name = stream_socket_get_name($server, false);
    fclose($server);

    if (!is_string($name) || !str_contains($name, ':')) {
        throw new RuntimeException('Unable to parse random port allocation.');
    }

    return (int) substr((string) strrchr($name, ':'), 1);
}

/**
 * @return array<string,string>
 */
function getenvAsArray(): array
{
    $values = getenv();
    if (!is_array($values)) {
        return [];
    }

    $result = [];
    foreach ($values as $key => $value) {
        if (is_string($key) && is_string($value)) {
            $result[$key] = $value;
        }
    }

    return $result;
}

function getenvString(string $name, string $default): string
{
    $value = getenv($name);
    if ($value === false || $value === '') {
        return $default;
    }
    return (string) $value;
}

function getenvInt(string $name): int
{
    $value = getenv($name);
    if (!is_string($value) || $value === '') {
        return 0;
    }
    return (int) $value;
}

function writeBenchmarkFile(array $payload): void
{
    $json = json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
    file_put_contents(dirname(__DIR__) . '/benchmark.json', $json . PHP_EOL);
}

function main(): int
{
    $mode = getenvString('BENCH_MODE', 'orchestrator');

    if ($mode === 'server') {
        return runServerWorker();
    }

    if ($mode === 'client') {
        return runClientWorker();
    }

    if ($mode === 'process') {
        return runProcessWorker();
    }

    $stageMs = getenvInt('BENCH_STAGE_MS');
    if ($stageMs <= 0) {
        $stageMs = DEFAULT_STAGE_MS;
    }

    $rpsPerClient = getenvInt('BENCH_RPS_PER_CLIENT');
    if ($rpsPerClient <= 0) {
        $rpsPerClient = DEFAULT_RPS_PER_CLIENT;
    }

    $startedAt = microtime(true);
    $payload = runOrchestrator($stageMs, $rpsPerClient, STAGE_CLIENTS);
    writeBenchmarkFile($payload);

    echo "Wrote benchmark.json\n";
    foreach ($payload['results'] as $row) {
        printf(
            "clients=%4d rps=%12.2f client(cpu=%7.2f%% mem=%7.2fMB) server(cpu=%7.2f%% mem=%7.2fMB)\n",
            $row['clients'],
            $row['rps'],
            $row['client']['cpu'],
            $row['client']['mem'],
            $row['server']['cpu'],
            $row['server']['mem'],
        );
    }
    printf("Elapsed %.2fs\n", microtime(true) - $startedAt);

    return 0;
}

exit(main());
