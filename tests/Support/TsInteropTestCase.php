<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Support;

abstract class TsInteropTestCase extends CoroutineTestCase
{
    public static function setUpBeforeClass(): void
    {
        TsServerFixture::start();
    }

    public static function tearDownAfterClass(): void
    {
        TsServerFixture::stop();
    }
}
