<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

/** A QUIC frame's length is over the reader's limit. */
final class FrameTooLarge extends \RuntimeException
{
}
