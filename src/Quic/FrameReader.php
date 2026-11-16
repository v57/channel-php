<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

/** Splits a QUIC stream back into frames. Zero-length frames are keepalives and are skipped. */
final class FrameReader
{
    private string $buffer = '';
    private bool $tooLarge = false;

    public function __construct(public readonly int $maxPayload = 16 * 1024 * 1024)
    {
    }

    /** A frame over the limit was seen: nothing more comes out of this reader. */
    public function failed(): bool
    {
        return $this->tooLarge;
    }

    /**
     * Adds $data and returns the frames it completed. A frame over the limit throws FrameTooLarge, after the
     * frames before it were returned (on the next call when this one returns some).
     *
     * @return list<string>
     */
    public function push(string $data): array
    {
        if ($this->tooLarge) {
            throw new FrameTooLarge('QUIC frame over the ' . $this->maxPayload . '-byte limit');
        }
        $this->buffer .= $data;
        $frames = [];
        $offset = 0;
        $available = strlen($this->buffer);
        while ($offset < $available) {
            $first = ord($this->buffer[$offset]);
            $size = 1 << ($first >> 6);
            if ($available - $offset < $size) {
                break;
            }
            $length = $first & 0x3f;
            for ($index = 1; $index < $size; $index++) {
                $length = ($length << 8) | ord($this->buffer[$offset + $index]);
            }
            // Refused as soon as the length is known, before buffering the payload
            if ($length > $this->maxPayload || $length < 0) {
                $this->tooLarge = true;
                $this->buffer = '';
                if ($frames === []) {
                    throw new FrameTooLarge('QUIC frame of ' . $length . ' bytes is over the ' . $this->maxPayload . '-byte limit');
                }
                return $frames;
            }
            if ($available - $offset - $size < $length) {
                break;
            }
            if ($length > 0) {
                $frames[] = substr($this->buffer, $offset + $size, $length);
            }
            $offset += $size + $length;
        }
        $this->buffer = $offset === $available ? '' : substr($this->buffer, $offset);
        return $frames;
    }
}
