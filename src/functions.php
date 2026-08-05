<?php
declare(strict_types=1);

namespace ChannelPhp;

function makeSender(Channel $channel, ConnectionInterface $connection): Sender
{
    return new Sender($channel, $connection);
}
