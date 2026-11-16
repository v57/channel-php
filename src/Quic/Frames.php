<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

/** The QUIC transport's frames (`channel/docs/quic.md`): a QUIC variable-length integer, then the payload. */
final class Frames
{
    public const ALPN = 'channel/1';

    public static function encode(string $payload): string
    {
        $length = strlen($payload);
        if ($length < 0x40) {
            return chr($length) . $payload;
        }
        if ($length < 0x4000) {
            return pack('n', 0x4000 | $length) . $payload;
        }
        if ($length < 0x40000000) {
            return pack('N', 0x80000000 | $length) . $payload;
        }
        return pack('J', (0xc0 << 56) | $length) . $payload;
    }

    /** The first frame a client sends: `{"hello": headers}`. @param array<string, string> $headers */
    public static function hello(array $headers): string
    {
        return self::encode(json_encode(['hello' => (object) $headers], JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    /**
     * Headers from a hello frame, names lowercased (a later name that lowercases the same wins), string values
     * only. Null when $frame isn't a hello.
     *
     * @return array<string, string>|null
     */
    public static function parseHello(string $frame): ?array
    {
        try {
            $decoded = json_decode($frame, false, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return null;
        }
        if (!is_object($decoded) || !isset($decoded->hello) || !is_object($decoded->hello)) {
            return null;
        }
        $headers = [];
        foreach (get_object_vars($decoded->hello) as $name => $value) {
            if (is_string($value)) {
                $headers[strtolower((string) $name)] = $value;
            }
        }
        return $headers;
    }
}
