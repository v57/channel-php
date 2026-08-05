<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Support;

use OpenSwoole\Coroutine;
use PHPUnit\Framework\TestCase;
use Throwable;

abstract class CoroutineTestCase extends TestCase
{
    protected function co(callable $callback): mixed
    {
        if (Coroutine::getCid() >= 0) {
            return $callback();
        }

        $result = null;
        $error = null;

        Coroutine::run(static function () use (&$result, &$error, $callback): void {
            try {
                $result = $callback();
            } catch (Throwable $throwable) {
                $error = $throwable;
            }
        });

        if ($error instanceof Throwable) {
            throw $error;
        }

        return $result;
    }
}
