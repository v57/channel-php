<?php
declare(strict_types=1);

namespace ChannelPhp\Internal;

/**
 * Incremental RFC 6455 frame reader for the client side.
 *
 * Written in PHP instead of using OpenSwoole's Coroutine\Http\Client::recv(), whose frame reader
 * segfaults the process on a declared 64-bit payload length of 2^64-1 (upstream bug, AUDIT §2.6 /
 * AUDIT2 §1.8). Lengths are checked before any arithmetic, messages are capped at `maxPayload`
 * bytes, and fragmented messages (FIN=0 + continuation frames) are reassembled.
 */
final class WebSocketFrameReader
{
    public const OPCODE_CONTINUATION = 0x0;
    public const OPCODE_TEXT = 0x1;
    public const OPCODE_BINARY = 0x2;
    public const OPCODE_CLOSE = 0x8;
    public const OPCODE_PING = 0x9;
    public const OPCODE_PONG = 0xA;

    public const CLOSE_PROTOCOL_ERROR = 1002;
    public const CLOSE_MESSAGE_TOO_BIG = 1009;

    private string $buffer = '';
    /** Read position in $buffer (avoids copying the buffer for every frame). */
    private int $pos = 0;
    private ?int $messageOpcode = null;
    private string $message = '';

    public function __construct(private readonly int $maxPayload)
    {
    }

    public function push(string $bytes): void
    {
        if ($this->pos > 0) {
            $this->buffer = (string) substr($this->buffer, $this->pos);
            $this->pos = 0;
        }
        $this->buffer .= $bytes;
    }

    /**
     * Next complete message or control frame, or null when more bytes are needed.
     *
     * @return array{opcode:int, data:string}|null
     * @throws WebSocketProtocolException
     */
    public function next(): ?array
    {
        while (true) {
            $p = $this->pos;
            $available = strlen($this->buffer) - $p;
            if ($available < 2) {
                return null;
            }

            $b0 = ord($this->buffer[$p]);
            $b1 = ord($this->buffer[$p + 1]);
            $fin = ($b0 & 0x80) !== 0;
            $opcode = $b0 & 0x0F;
            $masked = ($b1 & 0x80) !== 0;
            $length = $b1 & 0x7F;
            $offset = 2;

            if (($b0 & 0x70) !== 0) {
                throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'reserved bits set');
            }

            if ($length === 126) {
                if ($available < 4) {
                    return null;
                }
                $length = (ord($this->buffer[$p + 2]) << 8) | ord($this->buffer[$p + 3]);
                $offset = 4;
            } elseif ($length === 127) {
                if ($available < 10) {
                    return null;
                }
                // RFC 6455: the most significant bit must be 0. Check the high bytes before
                // building an integer so a 2^63..2^64-1 length can't overflow anything.
                if ((ord($this->buffer[$p + 2]) & 0x80) !== 0) {
                    throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'invalid frame length');
                }
                $length = 0;
                for ($i = 2; $i < 10; $i++) {
                    if ($length > ($this->maxPayload >> 8)) {
                        throw new WebSocketProtocolException(self::CLOSE_MESSAGE_TOO_BIG, 'message too big');
                    }
                    $length = ($length << 8) | ord($this->buffer[$p + $i]);
                }
                $offset = 10;
            }

            $isControl = ($opcode & 0x08) !== 0;
            if ($isControl && ($length > 125 || !$fin)) {
                throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'invalid control frame');
            }
            if (!$isControl && $length + strlen($this->message) > $this->maxPayload) {
                throw new WebSocketProtocolException(self::CLOSE_MESSAGE_TOO_BIG, 'message too big');
            }

            $maskKey = '';
            if ($masked) {
                if ($available < $offset + 4) {
                    return null;
                }
                $maskKey = substr($this->buffer, $p + $offset, 4);
                $offset += 4;
            }

            if ($available < $offset + $length) {
                return null;
            }

            $payload = (string) substr($this->buffer, $p + $offset, $length);
            $this->pos = $p + $offset + $length;
            if ($this->pos === strlen($this->buffer)) {
                $this->buffer = '';
                $this->pos = 0;
            }
            if ($masked && $length > 0) {
                $payload ^= str_repeat($maskKey, intdiv($length + 3, 4));
                $payload = substr($payload, 0, $length);
            }

            if ($isControl) {
                if (!in_array($opcode, [self::OPCODE_CLOSE, self::OPCODE_PING, self::OPCODE_PONG], true)) {
                    throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'unknown opcode ' . $opcode);
                }
                return ['opcode' => $opcode, 'data' => $payload];
            }

            if ($opcode === self::OPCODE_CONTINUATION) {
                if ($this->messageOpcode === null) {
                    throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'unexpected continuation frame');
                }
                $this->message .= $payload;
            } elseif ($opcode === self::OPCODE_TEXT || $opcode === self::OPCODE_BINARY) {
                if ($this->messageOpcode !== null) {
                    throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'expected continuation frame');
                }
                $this->messageOpcode = $opcode;
                $this->message = $payload;
            } else {
                throw new WebSocketProtocolException(self::CLOSE_PROTOCOL_ERROR, 'unknown opcode ' . $opcode);
            }

            if (!$fin) {
                continue;
            }

            $message = ['opcode' => $this->messageOpcode, 'data' => $this->message];
            $this->messageOpcode = null;
            $this->message = '';
            return $message;
        }
    }

    /** Client-to-server frame (always masked, RFC 6455 §5.3). */
    public static function encode(string $payload, int $opcode = self::OPCODE_TEXT): string
    {
        $length = strlen($payload);
        $header = chr(0x80 | $opcode);
        if ($length < 126) {
            $header .= chr(0x80 | $length);
        } elseif ($length < 65536) {
            $header .= chr(0x80 | 126) . pack('n', $length);
        } else {
            $header .= chr(0x80 | 127) . pack('J', $length);
        }

        $mask = random_bytes(4);
        if ($length === 0) {
            return $header . $mask;
        }

        $masked = $payload ^ str_repeat($mask, intdiv($length + 3, 4));
        return $header . $mask . substr($masked, 0, $length);
    }
}
