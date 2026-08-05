<?php
declare(strict_types=1);

namespace ChannelPhp;

final class RunningStream
{
    public bool $cancelled = false;

    public function __construct(public readonly mixed $iterator)
    {
    }

    public function return(): void
    {
        $this->cancelled = true;
    }
}
