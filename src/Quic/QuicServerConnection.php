<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

use ChannelPhp\ConnectionInterface;
use ChannelPhp\Controller;
use ChannelPhp\Internal\JsonCodec;
use ChannelPhp\Sender;
use ChannelPhp\SubscriptionEvent;
use ChannelPhp\TopicSubscriptions;
use FFI\CData;
use function ChannelPhp\makeSender;

/**
 * One connection of a QuicServer: its first bidirectional stream carries the channel connection, which starts
 * once the hello frame arrives. Each message is handled in a coroutine of its own, as OpenSwoole's WebSocket
 * server does.
 *
 * @internal
 */
final class QuicServerConnection implements ConnectionInterface
{
    private ?CData $stream = null;
    private readonly FrameReader $reader;
    private string $tx = '';
    private readonly TopicSubscriptions $subscriptions;
    /** @var array<string, bool> */
    private array $topics = [];

    public bool $closing = false;
    public float $closingDeadline = 0.0;
    private bool $helloSeen = false;
    /** Set once the hello's state is built; frames that arrive before wait in $waiting. */
    public bool $channelOpen = false;
    private bool $detached = false;
    private bool $freed = false;
    /** @var list<string> */
    private array $waiting = [];
    private mixed $state = null;
    private ?Sender $sender = null;
    private ?Controller $controller = null;

    public function __construct(private readonly QuicServer $server, private readonly OpenSsl $ossl, public readonly CData $ssl)
    {
        $this->reader = new FrameReader($server->maxPayload());
        $this->subscriptions = new TopicSubscriptions();
    }

    public function service(): void
    {
        $ffi = $this->ossl->ffi;
        $this->acceptStreams();
        if ($this->stream === null) {
            // The client went away before opening its stream
            $info = $ffi->new('char[64]');
            if ($ffi->SSL_get_conn_close_info($this->ssl, $info, 64) === 1) {
                $this->close();
            }
            $ffi->ERR_clear_error();
            return;
        }
        for ($iteration = 0; $iteration < 32 && !$this->closing; $iteration++) {
            $data = $this->ossl->read($this->stream, $this->server->chunk, 65536);
            if ($data === null) {
                $this->close();
                return;
            }
            if ($data !== '') {
                try {
                    $frames = $this->reader->push($data);
                } catch (FrameTooLarge) {
                    $this->close();
                    return;
                }
                foreach ($frames as $frame) {
                    if ($this->closing) {
                        return;
                    }
                    $this->frame($frame);
                }
                if ($this->reader->failed()) {
                    $this->close();
                    return;
                }
            }
            $pending = strlen($this->tx);
            if ($pending > 0 && !$this->ossl->flush($this->stream, $this->tx)) {
                $this->close();
                return;
            }
            if ($data === '' && strlen($this->tx) === $pending) {
                break;
            }
        }
    }

    /** One stream per connection: the client's first bidirectional one. Later and unidirectional ones are refused. */
    private function acceptStreams(): void
    {
        $ffi = $this->ossl->ffi;
        while (!OpenSsl::isNull($stream = $ffi->SSL_accept_stream($this->ssl, OpenSsl::ACCEPT_STREAM_NO_BLOCK))) {
            if ($this->stream === null && $ffi->SSL_get_stream_type($stream) === OpenSsl::STREAM_TYPE_BIDI) {
                $this->stream = $stream;
                $ffi->SSL_ctrl($stream, OpenSsl::CTRL_MODE, OpenSsl::MODE_ENABLE_PARTIAL_WRITE | OpenSsl::MODE_ACCEPT_MOVING_WRITE_BUFFER, null);
                $ffi->SSL_set_incoming_stream_policy($this->ssl, OpenSsl::INCOMING_STREAM_POLICY_REJECT, 0);
                continue;
            }
            // OpenSSL can't lower the stream limits: any later stream is reset (as in channel-c and
            // channel-zig); the first one keeps working
            $args = $ffi->new('uint64_t');
            $args->cdata = 0;
            $ffi->SSL_stream_reset($stream, \FFI::addr($args), 8);
            $ffi->SSL_free($stream);
        }
        $ffi->ERR_clear_error();
    }

    private function frame(string $frame): void
    {
        if (!$this->helloSeen) {
            $this->helloSeen = true;
            $headers = Frames::parseHello($frame);
            if ($headers === null) {
                $this->close();
                return;
            }
            // The state factory may wait (a coroutine call); frames that arrive meanwhile are kept in order
            go(function () use ($headers): void {
                $this->open($headers);
            });
            return;
        }
        if (!$this->channelOpen) {
            $this->waiting[] = $frame;
            return;
        }
        go(function () use ($frame): void {
            $this->receive($frame);
        });
    }

    /** @param array<string, string> $headers */
    private function open(array $headers): void
    {
        $factory = $this->server->options['state'] ?? null;
        try {
            $state = is_callable($factory) ? $factory($headers) : new \stdClass();
        } catch (\Throwable) {
            $this->close();
            return;
        }
        if ($this->closing) {
            return;
        }
        $this->state = $state;
        $this->sender = makeSender($this->server->channel, $this);
        $this->controller = new Controller(
            responseImpl: function (array $response): void {
                $this->notify($response);
            },
            subscribeImpl: function (string $topic): void {
                $this->topics[$topic] = true;
            },
            unsubscribeImpl: function (string $topic): void {
                unset($this->topics[$topic]);
            },
            eventImpl: function (string $topic, mixed $event): void {
                $this->subscriptions->receivedEvent($topic, $event);
            },
            sender: $this->sender,
            state: $state,
        );
        $this->channelOpen = true;
        $onConnect = $this->server->options['onConnect'] ?? null;
        if (is_callable($onConnect)) {
            $onConnect($this->context());
        }
        $waiting = $this->waiting;
        $this->waiting = [];
        foreach ($waiting as $frame) {
            if ($this->closing) {
                return;
            }
            go(function () use ($frame): void {
                $this->receive($frame);
            });
        }
        $this->server->schedule();
    }

    /** @return array{state: mixed, sender: Sender, subscriptions: TopicSubscriptions} */
    private function context(): array
    {
        return ['state' => $this->state, 'sender' => $this->sender, 'subscriptions' => $this->subscriptions];
    }

    private function receive(string $frame): void
    {
        if ($this->closing || $this->controller === null) {
            return;
        }
        try {
            $payload = JsonCodec::decode($frame);
        } catch (\Throwable) {
            // Invalid JSON closes the connection
            $this->close();
            return;
        }
        try {
            $this->server->channel->receive($payload, $this->controller);
        } catch (\Throwable) {
            $this->close();
        }
    }

    public function publish(SubscriptionEvent $event): void
    {
        if ($this->channelOpen && !$this->closing && isset($this->topics[$event->topic])) {
            $this->sendText(JsonCodec::encode(['topic' => $event->topic, 'body' => $event->body]));
        }
    }

    private function sendText(string $text): void
    {
        if ($this->closing) {
            return;
        }
        $this->tx .= Frames::encode($text);
        $this->server->schedule();
    }

    public function send(array $body): mixed
    {
        $this->sendText(JsonCodec::encode($body));
        return 0;
    }

    public function sent(int $id): void
    {
    }

    public function cancel(int $id): bool
    {
        return false;
    }

    public function notify(array $body): void
    {
        $this->sendText(JsonCodec::encode($body));
    }

    public function addTopic(string $topic, callable $event): callable
    {
        return $this->subscriptions->addTopic($topic, $event);
    }

    public function stop(): void
    {
        $this->close();
    }

    /** Ends the connection: what the handlers already sent still goes out before CONNECTION_CLOSE. */
    public function close(): void
    {
        if ($this->closing) {
            return;
        }
        $this->closing = true;
        $this->closingDeadline = microtime(true) + 2.0;
        if ($this->stream !== null) {
            $this->ossl->flush($this->stream, $this->tx);
            $this->ossl->ffi->SSL_stream_conclude($this->stream, 0);
            $this->ossl->ffi->ERR_clear_error();
        }
        $this->server->schedule();
    }

    /** Ends the channel connection once: pending requests fail, onDisconnect runs. */
    public function detach(): void
    {
        $this->closing = true;
        if ($this->detached) {
            return;
        }
        $this->detached = true;
        if (!$this->channelOpen || $this->sender === null) {
            return;
        }
        $this->channelOpen = false;
        $this->server->channel->disconnect($this->state, $this->sender);
        $onDisconnect = $this->server->options['onDisconnect'] ?? null;
        if (is_callable($onDisconnect)) {
            try {
                $onDisconnect($this->context());
            } catch (\Throwable) {
            }
        }
    }

    public function free(): void
    {
        if ($this->freed) {
            return;
        }
        $this->freed = true;
        $ffi = $this->ossl->ffi;
        if ($this->stream !== null) {
            $ffi->SSL_free($this->stream);
            $this->stream = null;
        }
        $ffi->SSL_free($this->ssl);
    }
}
