<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

/** ext-ffi or OpenSSL 3.5+ can't be loaded. */
final class QuicUnavailable extends \RuntimeException
{
}
