<?php
declare(strict_types=1);

require dirname(__DIR__) . '/vendor/autoload.php';

if (!class_exists(OpenSwoole\Coroutine::class)) {
    throw new RuntimeException('ext-openswoole is required to run tests.');
}
