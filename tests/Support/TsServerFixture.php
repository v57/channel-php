<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Support;

final class TsServerFixture
{
    private static $process = null;

    /** @var array<int, resource> */
    private static array $pipes = [];

    public static function start(): void
    {
        if (self::$process !== null) {
            return;
        }

        $root = dirname(__DIR__, 3);
        $cwd = $root . '/channel';

        $descriptorSpec = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open('bun tests/server.ts', $descriptorSpec, $pipes, $cwd);
        if (!is_resource($process)) {
            throw new \RuntimeException('Failed to start TS fixture server process');
        }

        self::$process = $process;
        self::$pipes = $pipes;

        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        if (!self::waitForPort('127.0.0.1', 2049, 10.0)) {
            $stderr = stream_get_contents($pipes[2]) ?: '';
            self::stop();
            throw new \RuntimeException('TS fixture server did not start on 127.0.0.1:2049. ' . $stderr);
        }
    }

    public static function stop(): void
    {
        if (self::$process !== null && is_resource(self::$process)) {
            $status = proc_get_status(self::$process);
            if (!empty($status['running'])) {
                proc_terminate(self::$process, SIGTERM);

                $attempt = 0;
                while ($attempt < 100) {
                    $status = proc_get_status(self::$process);
                    if (empty($status['running'])) {
                        break;
                    }
                    usleep(10_000);
                    $attempt++;
                }

                $status = proc_get_status(self::$process);
                if (!empty($status['running'])) {
                    proc_terminate(self::$process, SIGKILL);
                }
            }

            proc_close(self::$process);
        }

        foreach (self::$pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }

        self::$process = null;
        self::$pipes = [];
    }

    private static function waitForPort(string $host, int $port, float $timeoutSeconds): bool
    {
        $deadline = microtime(true) + $timeoutSeconds;
        while (microtime(true) < $deadline) {
            $connection = @stream_socket_client(
                sprintf('tcp://%s:%d', $host, $port),
                $errno,
                $errstr,
                0.2,
            );

            if (is_resource($connection)) {
                fclose($connection);
                return true;
            }

            usleep(50_000);
        }

        return false;
    }
}
