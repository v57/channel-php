<?php
declare(strict_types=1);

namespace ChannelPhp;

final class SubscriptionHandle implements Cancellable
{
    public function __construct(private readonly mixed $cancelImpl)
    {
    }

    public function cancel(): void
    {
        ($this->cancelImpl)();
    }
}
