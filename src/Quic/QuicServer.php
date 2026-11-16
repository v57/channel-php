<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

use ChannelPhp\Channel;
use ChannelPhp\SubscriptionEvent;
use ChannelPhp\Transports\WebSocketClient;
use FFI;
use FFI\CData;
use OpenSwoole\Coroutine;

/**
 * Channel over QUIC (experimental, `channel/docs/quic.md`): a listener on one UDP port, OpenSSL 3.5's native QUIC
 * through ext-ffi. It runs in this process on OpenSwoole's reactor (unlike listen(), which forks a WebSocket
 * server), so start it inside a coroutine context (Coroutine::run, a server's worker) and stop() it there.
 */
final class QuicServer
{
    private readonly OpenSsl $ossl;
    private readonly FFI $ffi;
    private ?CData $ctx = null;
    private ?CData $listener = null;
    private ?CData $alpn = null;
    /** Kept alive while OpenSSL may call it */
    private ?\Closure $alpnCallback = null;
    private int $fd = -1;
    private int $port = 0;
    private bool $running = false;
    private bool $watching = false;

    /** @var array<int, QuicServerConnection> */
    private array $connections = [];

    /** @var list<array{0: \ChannelPhp\Subscription, 1: \Closure}> */
    private array $publishers = [];

    public readonly CData $chunk;

    private int|false|null $timer = null;
    private float $timerDeadline = 0.0;
    private bool $servicing = false;
    private bool $again = false;
    private bool $deferred = false;

    /**
     * @param array{cert: string, key: string, state?: callable(array<string, string>): mixed, onConnect?: callable,
     *     onDisconnect?: callable, maxPayload?: int} $options
     */
    private function __construct(public readonly Channel $channel, public readonly array $options)
    {
        $this->ossl = OpenSsl::instance();
        $this->ffi = $this->ossl->ffi;
        $this->chunk = $this->ffi->new('unsigned char[65536]');
    }

    /**
     * Listens on UDP `$address` (a port, or `host:port`; 127.0.0.1 by default, port 0 picks a free one).
     * Options: `cert` and `key` (PEM text, required: QUIC has no plaintext mode), and as for listen(): `state`
     * (built from the hello's headers, names lowercased), `onConnect`, `onDisconnect`, `maxPayload`.
     */
    public static function listen(Channel $channel, int|string $address, array $options): self
    {
        if (Coroutine::getCid() < 0) {
            throw new \RuntimeException('listenQuic() runs on the OpenSwoole reactor: call it inside a coroutine (Coroutine::run).');
        }
        foreach (['cert', 'key'] as $name) {
            if (!isset($options[$name]) || !is_string($options[$name]) || $options[$name] === '') {
                throw new \InvalidArgumentException("listenQuic needs the `$name` option (PEM)");
            }
        }
        $host = '127.0.0.1';
        if (is_int($address)) {
            $port = $address;
        } else {
            $colon = strrpos($address, ':');
            if ($colon === false || $colon === 0) {
                throw new \InvalidArgumentException("invalid listen address: $address (host:port)");
            }
            $host = trim(substr($address, 0, $colon), '[]');
            $port = (int) substr($address, $colon + 1);
        }
        if ($port < 0 || $port > 65535) {
            throw new \InvalidArgumentException("port out of range: $address");
        }
        $server = new self($channel, $options);
        $server->start($host, $port);
        return $server;
    }

    /** The UDP port it listens on (the real one when asked for port 0). */
    public function port(): int
    {
        return $this->port;
    }

    /** Open connections (hello received). */
    public function connectionCount(): int
    {
        return count(array_filter($this->connections, static fn(QuicServerConnection $c): bool => $c->channelOpen));
    }

    public function maxPayload(): int
    {
        return (int) ($this->options['maxPayload'] ?? WebSocketClient::DEFAULT_MAX_PAYLOAD);
    }

    private function start(string $host, int $port): void
    {
        $ffi = $this->ffi;
        try {
            $this->ctx = $ffi->SSL_CTX_new($ffi->OSSL_QUIC_server_method());
            if (OpenSsl::isNull($this->ctx)) {
                throw new \RuntimeException($this->ossl->error('failed to create a quic context'));
            }
            $this->loadIdentity();
            $ffi->SSL_CTX_set_verify($this->ctx, OpenSsl::VERIFY_NONE, null);
            $this->alpn = $this->ossl->alpn();
            $alpn = $this->alpn;
            $this->alpnCallback = static function ($ssl, $out, $outLength, $input, $inputLength, $arg) use ($ffi, $alpn): int {
                $result = $ffi->SSL_select_next_proto($out, $outLength, $alpn, strlen(Frames::ALPN) + 1, $input, $inputLength);
                return $result === OpenSsl::NPN_NEGOTIATED ? OpenSsl::TLSEXT_ERR_OK : OpenSsl::TLSEXT_ERR_ALERT_FATAL;
            };
            $ffi->SSL_CTX_set_alpn_select_cb($this->ctx, $this->alpnCallback, null);
            // OpenSSL's default max_idle_timeout is the protocol's 30 s

            $this->listener = $ffi->SSL_new_listener($this->ctx, 0);
            if (OpenSsl::isNull($this->listener)) {
                throw new \RuntimeException($this->ossl->error('failed to create a quic listener'));
            }
            [$this->fd, $this->port] = $this->ossl->udpSocket($this->listener, $host, $port, true);
            if ($this->port <= 0) {
                $this->port = $port;
            }
            if ($ffi->SSL_set_blocking_mode($this->listener, 0) !== 1 || $ffi->SSL_listen($this->listener) !== 1) {
                throw new \RuntimeException($this->ossl->error('failed to listen'));
            }
        } catch (\Throwable $error) {
            $this->free();
            throw $error;
        }
        $this->running = true;
        \OpenSwoole\Event::add($this->fd, function (): void {
            $this->tick();
        });
        $this->watching = true;

        foreach ($this->channel->eventsApi ?? [] as $subscription) {
            $publisher = function (SubscriptionEvent $event): void {
                foreach ($this->connections as $connection) {
                    $connection->publish($event);
                }
            };
            $subscription->publishers[] = $publisher;
            $this->publishers[] = [$subscription, $publisher];
        }
        $this->arm();
    }

    private function loadIdentity(): void
    {
        $ffi = $this->ffi;
        $certificates = $this->ossl->readCertificates((string) $this->options['cert']);
        if ($certificates === [] || $ffi->SSL_CTX_use_certificate($this->ctx, $certificates[0]) !== 1) {
            foreach ($certificates as $certificate) {
                $ffi->X509_free($certificate);
            }
            throw new \InvalidArgumentException('invalid certificate');
        }
        $ffi->X509_free($certificates[0]);
        foreach (array_slice($certificates, 1) as $chain) {
            // SSL_CTX_add0_chain_cert takes ownership
            if ($ffi->SSL_CTX_ctrl($this->ctx, OpenSsl::CTRL_CHAIN_CERT, 0, $chain) !== 1) {
                $ffi->X509_free($chain);
            }
        }
        $key = $this->ossl->readPrivateKey((string) $this->options['key']);
        $ok = $key !== null && $ffi->SSL_CTX_use_PrivateKey($this->ctx, $key) === 1 && $ffi->SSL_CTX_check_private_key($this->ctx) === 1;
        if ($key !== null) {
            $ffi->EVP_PKEY_free($key);
        }
        $ffi->ERR_clear_error();
        if (!$ok) {
            throw new \InvalidArgumentException("invalid key or doesn't match the certificate");
        }
    }

    /** Runs service() once the current work is done, so everything sent meanwhile goes out together. */
    public function schedule(): void
    {
        if ($this->deferred || !$this->running) {
            return;
        }
        $this->deferred = true;
        \OpenSwoole\Event::defer(function (): void {
            $this->deferred = false;
            $this->tick();
        });
    }

    private function tick(): void
    {
        if (!$this->running) {
            return;
        }
        if (Coroutine::getCid() >= 0) {
            $this->service();
            return;
        }
        go(function (): void {
            $this->service();
        });
    }

    private function service(): void
    {
        if (!$this->running) {
            return;
        }
        if ($this->servicing) {
            $this->again = true;
            return;
        }
        $this->servicing = true;
        $ffi = $this->ffi;
        try {
            do {
                $this->again = false;
                $ffi->SSL_handle_events($this->listener);
                while (!OpenSsl::isNull($ssl = $ffi->SSL_accept_connection($this->listener, OpenSsl::ACCEPT_CONNECTION_NO_BLOCK))) {
                    $connection = new QuicServerConnection($this, $this->ossl, $ssl);
                    if ($ffi->SSL_set_blocking_mode($ssl, 0) !== 1
                        || $ffi->SSL_set_default_stream_mode($ssl, OpenSsl::DEFAULT_STREAM_MODE_NONE) !== 1
                        || $ffi->SSL_set_incoming_stream_policy($ssl, OpenSsl::INCOMING_STREAM_POLICY_ACCEPT, 0) !== 1) {
                        $connection->close();
                    }
                    $this->connections[spl_object_id($connection)] = $connection;
                }
                $ffi->ERR_clear_error();
                foreach ($this->connections as $connection) {
                    if (!$connection->closing) {
                        $connection->service();
                    }
                }
                $this->reap();
            } while ($this->again && $this->running);
        } finally {
            $this->servicing = false;
        }
        $this->arm();
    }

    /** Frees connections that finished closing (or gave up after 2 s). */
    private function reap(): void
    {
        foreach ($this->connections as $id => $connection) {
            if (!$connection->closing) {
                continue;
            }
            $connection->detach();
            $done = $this->ffi->SSL_shutdown_ex($connection->ssl, OpenSsl::SHUTDOWN_FLAG_NO_BLOCK, null, 0) !== 0
                || microtime(true) > $connection->closingDeadline;
            $this->ffi->ERR_clear_error();
            if ($done) {
                $connection->free();
                unset($this->connections[$id]);
            }
        }
    }

    private function arm(): void
    {
        if (!$this->running) {
            return;
        }
        $timeout = $this->ossl->eventTimeoutMs($this->listener);
        // Closing connections are checked again until they finish
        foreach ($this->connections as $connection) {
            if ($connection->closing) {
                $timeout = $timeout === null ? 50 : min($timeout, 50);
                break;
            }
        }
        if ($this->ffi->SSL_net_write_desired($this->listener) === 1) {
            $timeout = 1;
        }
        if ($timeout === null) {
            return;
        }
        $timeout = max(1, $timeout);
        $deadline = microtime(true) + $timeout / 1000;
        if (is_int($this->timer)) {
            if ($this->timerDeadline <= $deadline + 0.0005) {
                return;
            }
            \OpenSwoole\Timer::clear($this->timer);
        }
        $this->timerDeadline = $deadline;
        $this->timer = \OpenSwoole\Timer::after($timeout, function (): void {
            $this->timer = null;
            $this->tick();
        });
    }

    /**
     * Closes every connection (CONNECTION_CLOSE, without waiting for the peers) and frees the port, all at
     * once, so no client gets to reconnect to a listener about to go.
     */
    public function stop(): void
    {
        if (!$this->running) {
            return;
        }
        $this->running = false;
        $this->unwatch();
        foreach ($this->publishers as [$subscription, $publisher]) {
            $index = array_search($publisher, $subscription->publishers, true);
            if ($index !== false) {
                unset($subscription->publishers[$index]);
                $subscription->publishers = array_values($subscription->publishers);
            }
        }
        $this->publishers = [];
        $ffi = $this->ffi;
        // The ones OpenSSL accepted but this server hasn't taken yet too
        $unaccepted = [];
        while (!OpenSsl::isNull($ssl = $ffi->SSL_accept_connection($this->listener, OpenSsl::ACCEPT_CONNECTION_NO_BLOCK))) {
            $unaccepted[] = $ssl;
        }
        foreach ($this->connections as $connection) {
            $connection->detach();
        }
        $closing = [...array_map(static fn(QuicServerConnection $c): CData => $c->ssl, array_values($this->connections)), ...$unaccepted];
        foreach ($closing as $ssl) {
            $ffi->SSL_shutdown_ex($ssl, OpenSsl::SHUTDOWN_FLAG_RAPID | OpenSsl::SHUTDOWN_FLAG_NO_BLOCK, null, 0);
        }
        for ($round = 0; $round < 3 && $closing !== []; $round++) {
            $ffi->SSL_handle_events($this->listener);
        }
        foreach ($unaccepted as $ssl) {
            $ffi->SSL_free($ssl);
        }
        foreach ($this->connections as $connection) {
            $connection->free();
        }
        $this->connections = [];
        $this->free();
    }

    private function unwatch(): void
    {
        if ($this->watching) {
            \OpenSwoole\Event::del($this->fd);
            $this->watching = false;
        }
        if (is_int($this->timer)) {
            \OpenSwoole\Timer::clear($this->timer);
            $this->timer = null;
        }
    }

    private function free(): void
    {
        $this->running = false;
        $this->unwatch();
        $ffi = $this->ffi;
        $ffi->ERR_clear_error();
        if ($this->listener !== null && !OpenSsl::isNull($this->listener)) {
            $ffi->SSL_free($this->listener);
        }
        $this->listener = null;
        if ($this->ctx !== null && !OpenSsl::isNull($this->ctx)) {
            $ffi->SSL_CTX_free($this->ctx);
        }
        $this->ctx = null;
    }
}
