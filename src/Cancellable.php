<?php
declare(strict_types=1);

namespace ChannelPhp;

interface Cancellable
{
    public function cancel(): void;
}
