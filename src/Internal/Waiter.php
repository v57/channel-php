<?php
declare(strict_types=1);

namespace ChannelPhp\Internal;

use ChannelPhp\ChannelException;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Channel;
use Throwable;

final class Waiter
{
    private bool $resolved = false;
    private mixed $value = null;
    private mixed $error = null;
    private ?Channel $channel = null;

    public function __construct()
    {
        if (Coroutine::getCid() >= 0) {
            $this->channel = new Channel(1);
        }
    }

    public function resolve(mixed $value): void
    {
        if ($this->resolved) {
            return;
        }

        $this->resolved = true;
        $this->value = $value;
        if ($this->channel !== null) {
            $this->channel->push(true);
        }
    }

    public function reject(mixed $error): void
    {
        if ($this->resolved) {
            return;
        }

        $this->resolved = true;
        $this->error = $error;
        if ($this->channel !== null) {
            $this->channel->push(true);
        }
    }

    public function await(): mixed
    {
        while (!$this->resolved) {
            if ($this->channel !== null) {
                $this->channel->pop();
                break;
            }

            usleep(1_000);
        }

        if ($this->error !== null) {
            if ($this->error instanceof Throwable) {
                throw $this->error;
            }
            throw new ChannelException((string) $this->error);
        }

        return $this->value;
    }
}
