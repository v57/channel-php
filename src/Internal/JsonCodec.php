<?php
declare(strict_types=1);

namespace ChannelPhp\Internal;

use JsonException;

final class JsonCodec
{
    public static function encode(mixed $body): string
    {
        return json_encode(
            $body,
            JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES,
        );
    }

    public static function decode(string $json): mixed
    {
        if (getenv('CHANNEL_PHP_DISABLE_SIMDJSON') !== '1' && function_exists('simdjson_decode')) {
            // ext-simdjson is significantly faster than json_decode.
            return simdjson_decode($json, true, 512);
        }

        return json_decode($json, true, 512, JSON_THROW_ON_ERROR);
    }
}
