<?php
declare(strict_types=1);

namespace ChannelPhp;

use RuntimeException;

final class ChannelException extends RuntimeException
{
    /** Message for an `error` field: strings as-is, anything else (e.g. an error object) as JSON. */
    public static function messageOf(mixed $error): string
    {
        if (is_string($error)) {
            return $error;
        }
        if ($error instanceof \Stringable) {
            return (string) $error;
        }
        $json = json_encode($error, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PARTIAL_OUTPUT_ON_ERROR);
        return $json === false ? get_debug_type($error) : $json;
    }

    public static function from(mixed $error): self
    {
        return new self(self::messageOf($error));
    }
}
