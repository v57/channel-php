<?php
declare(strict_types=1);

namespace ChannelPhp;

use ChannelPhp\Internal\Waiter;

final class RequestHandle implements Cancellable
{
    public function __construct(
        private readonly Waiter $waiter,
        private readonly mixed $cancelImpl,
    ) {
    }

    public function await(): mixed
    {
        return $this->waiter->await();
    }

    public function cancel(): void
    {
        ($this->cancelImpl)();
    }
}
