<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

final class RunningServer
{
    private bool $stopped = false;

    public function __construct(
        private readonly int $pid,
        public readonly string $host,
        public readonly int $port,
    ) {
    }

    public function pid(): int
    {
        return $this->pid;
    }

    public function stop(): void
    {
        if ($this->stopped) {
            return;
        }
        $this->stopped = true;

        @posix_kill($this->pid, SIGTERM);

        $attempts = 0;
        while ($attempts < 200) {
            $result = pcntl_waitpid($this->pid, $status, WNOHANG);
            if ($result === $this->pid || $result === -1) {
                return;
            }
            usleep(10_000);
            $attempts++;
        }

        @posix_kill($this->pid, SIGKILL);
        @pcntl_waitpid($this->pid, $status);
    }

    public function __destruct()
    {
        $this->stop();
    }
}
