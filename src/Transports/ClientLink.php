<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

/**
 * One open connection of a WebSocketClient made by a dialer instead of its own WebSocket code: the QUIC transport
 * (ChannelPhp\Quic\QuicLink) uses it.
 */
interface ClientLink
{
    /** Sends one text message; false when the link is gone. */
    public function sendText(string $text): bool;

    /**
     * Waits, in this coroutine, for the next text message. Null when the link closed; a ClientLinkError when the
     * peer broke the protocol.
     *
     * @throws ClientLinkError
     */
    public function receive(): ?string;

    public function close(): void;
}
