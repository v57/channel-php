<?php
declare(strict_types=1);

namespace ChannelPhp;

final class SubscriptionEvent
{
    public function __construct(
        public readonly string $topic,
        public readonly mixed $body,
    ) {
    }
}
