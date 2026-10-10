<?php
declare(strict_types=1);

namespace ChannelPhp\Internal;

/** A peer broke the WebSocket protocol; `closeCode` is the RFC 6455 close code to send. */
final class WebSocketProtocolException extends \RuntimeException
{
    public function __construct(public readonly int $closeCode, string $reason)
    {
        parent::__construct($reason);
    }
}
