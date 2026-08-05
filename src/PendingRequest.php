<?php
declare(strict_types=1);

namespace ChannelPhp;

final class PendingRequest
{
    public function __construct(
        public array $request,
        public readonly mixed $response,
        public readonly ?Sender $sender = null,
    ) {
    }
}
