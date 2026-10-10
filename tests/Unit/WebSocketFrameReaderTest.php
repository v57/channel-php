<?php
declare(strict_types=1);

namespace ChannelPhp\Tests\Unit;

use ChannelPhp\Internal\WebSocketFrameReader;
use ChannelPhp\Internal\WebSocketProtocolException;
use PHPUnit\Framework\TestCase;

final class WebSocketFrameReaderTest extends TestCase
{
    private static function serverFrame(string $payload, int $opcode = 1, bool $fin = true): string
    {
        $length = strlen($payload);
        $b0 = chr(($fin ? 0x80 : 0) | $opcode);
        if ($length < 126) {
            return $b0 . chr($length) . $payload;
        }
        if ($length < 65536) {
            return $b0 . chr(126) . pack('n', $length) . $payload;
        }
        return $b0 . chr(127) . pack('J', $length) . $payload;
    }

    public function testDecodesTextFramesFedByteByByte(): void
    {
        $reader = new WebSocketFrameReader(1 << 20);
        $bytes = self::serverFrame('{"id":1}') . self::serverFrame(str_repeat('a', 300)) . self::serverFrame(str_repeat('b', 70000));
        $messages = [];
        foreach (str_split($bytes, 7) as $chunk) {
            $reader->push($chunk);
            while (($frame = $reader->next()) !== null) {
                $messages[] = $frame['data'];
            }
        }
        self::assertSame(['{"id":1}', str_repeat('a', 300), str_repeat('b', 70000)], $messages);
    }

    public function testReassemblesFragmentedMessagesAroundControlFrames(): void
    {
        $reader = new WebSocketFrameReader(1 << 20);
        $reader->push(self::serverFrame('{"id"', 1, false) . self::serverFrame('hi', 9) . self::serverFrame(':5}', 0, true));
        self::assertSame(['opcode' => 9, 'data' => 'hi'], $reader->next());
        self::assertSame(['opcode' => 1, 'data' => '{"id":5}'], $reader->next());
        self::assertNull($reader->next());
    }

    public function testUnmasksMaskedFramesAndRoundTripsEncode(): void
    {
        $reader = new WebSocketFrameReader(1 << 20);
        foreach (['', 'x', str_repeat('z', 200), str_repeat('q', 70000)] as $payload) {
            $reader->push(WebSocketFrameReader::encode($payload));
            self::assertSame(['opcode' => 1, 'data' => $payload], $reader->next());
        }
    }

    /** AUDIT §2.6 / AUDIT2 §1.8: these lengths segfault OpenSwoole's own frame reader. */
    public function testHugeDeclaredLengthsAreRejectedWithoutReadingThePayload(): void
    {
        foreach (['ffffffffffffffff' => 1002, '7fffffffffffffff' => 1009, '0000000040000000' => 1009] as $hex => $code) {
            $reader = new WebSocketFrameReader(16 * 1024 * 1024);
            $reader->push("\x81\x7f" . hex2bin($hex));
            try {
                $reader->next();
                self::fail('expected protocol error for ' . $hex);
            } catch (WebSocketProtocolException $error) {
                self::assertSame($code, $error->closeCode, $hex);
            }
        }
    }

    public function testFragmentsCountTowardsTheMessageLimit(): void
    {
        $reader = new WebSocketFrameReader(10);
        $reader->push(self::serverFrame('123456', 1, false) . self::serverFrame('789012', 0, true));
        $this->expectException(WebSocketProtocolException::class);
        $reader->next();
    }

    public function testRejectsProtocolViolations(): void
    {
        foreach ([
            self::serverFrame('x', 0),                          // continuation without start
            self::serverFrame('a', 1, false) . self::serverFrame('b', 1), // new message mid-fragment
            self::serverFrame('x', 3),                          // reserved opcode
            self::serverFrame(str_repeat('p', 126), 9),         // oversized control frame
            "\xc1\x01x",                                        // RSV1 without extension
        ] as $bytes) {
            $reader = new WebSocketFrameReader(1 << 20);
            $reader->push($bytes);
            try {
                while ($reader->next() !== null) {
                }
                self::fail('expected protocol error for ' . bin2hex($bytes));
            } catch (WebSocketProtocolException $error) {
                self::assertSame(1002, $error->closeCode);
            }
        }
    }
}
