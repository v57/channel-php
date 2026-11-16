<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

/** The peer of a ClientLink broke the protocol (a frame over the limit, for example). */
final class ClientLinkError extends \RuntimeException
{
}
