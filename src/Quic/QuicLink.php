<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

use ChannelPhp\Transports\ClientLink;
use ChannelPhp\Transports\ClientLinkError;
use FFI;
use FFI\CData;
use OpenSwoole\Coroutine;
use OpenSwoole\Coroutine\Channel as CoChannel;

/**
 * One client QUIC connection, from its own UDP socket: the hello, then text messages as frames on one stream.
 * OpenSSL owns the socket; OpenSwoole's reactor watches it and a timer covers OpenSSL's timeouts and the
 * keepalive, so nothing polls.
 */
final class QuicLink implements ClientLink
{
    private const PHASE_CONNECTING = 0;
    private const PHASE_OPEN = 1;
    private const PHASE_CLOSED = 2;

    private readonly OpenSsl $ossl;
    private readonly FFI $ffi;
    private ?CData $ctx = null;
    private ?CData $ssl = null;
    private ?CData $stream = null;
    /** @var list<CData> streams opened by writeOnNewStream (tests) */
    private array $extraStreams = [];
    private int $fd = -1;
    private bool $watching = false;
    private int $phase = self::PHASE_CONNECTING;
    private ?string $failure = null;
    private ?string $protocolError = null;

    private string $tx = '';
    private readonly FrameReader $reader;
    private readonly CData $chunk;
    /** @var list<string> */
    private array $inbox = [];
    private ?CoChannel $waiter = null;
    private ?CoChannel $opened = null;

    private int|false|null $timer = null;
    private float $timerDeadline = 0.0;
    private float $lastSent = 0.0;
    private bool $servicing = false;
    private bool $again = false;
    private bool $deferred = false;

    /**
     * @param array<string, string> $headers sent in the hello
     */
    private function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly array $quic,
        private readonly ?string $pin,
        private readonly array $headers,
        int $maxPayload,
        private readonly bool $raw = false,
    ) {
        $this->ossl = OpenSsl::instance();
        $this->ffi = $this->ossl->ffi;
        $this->reader = new FrameReader($maxPayload);
        $this->chunk = $this->ffi->new('unsigned char[65536]');
    }

    /**
     * A WebSocketClient dialer for `quic://` addresses. Checks the address and options right away (so a mistake
     * throws instead of being retried) and needs ext-ffi and OpenSSL 3.5+.
     *
     * @param array{ca?: ?string, fingerprint?: ?string, servername?: ?string} $quic
     * @return \Closure(string, array<string, string>): ?ClientLink
     */
    public static function dialer(string $address, array $quic, int $maxPayload): \Closure
    {
        [$host, $port] = self::parseAddress($address);
        $pin = isset($quic['fingerprint']) && $quic['fingerprint'] !== null ? self::parseFingerprint((string) $quic['fingerprint']) : null;
        foreach (array_keys($quic) as $key) {
            if (!in_array($key, ['ca', 'fingerprint', 'servername'], true)) {
                throw new \InvalidArgumentException("unknown quic option: $key");
            }
        }
        OpenSsl::instance();
        return static function (string $_address, array $headers) use ($host, $port, $quic, $pin, $maxPayload): ?ClientLink {
            $link = new self($host, $port, $quic, $pin, $headers, $maxPayload);
            return $link->open() ? $link : null;
        };
    }

    /** A connection that sends raw bytes and no hello, for testing a server's wire rules. */
    public static function raw(string $address, array $quic): self
    {
        [$host, $port] = self::parseAddress($address);
        $pin = isset($quic['fingerprint']) ? self::parseFingerprint((string) $quic['fingerprint']) : null;
        $link = new self($host, $port, $quic, $pin, [], 16 * 1024 * 1024, true);
        if (!$link->open()) {
            throw new \RuntimeException('QUIC connect failed: ' . ($link->failure ?? 'unknown'));
        }
        return $link;
    }

    /** Why the last open() failed. */
    public function failure(): ?string
    {
        return $this->failure;
    }

    /**
     * `quic://host:port[/...]`, IPv6 hosts in brackets.
     *
     * @return array{0: string, 1: int}
     */
    public static function parseAddress(string $address): array
    {
        $parts = parse_url($address);
        if ($parts === false || ($parts['scheme'] ?? '') !== 'quic' || !isset($parts['host'], $parts['port']) || $parts['host'] === '') {
            throw new \InvalidArgumentException("invalid quic address: $address (quic://host:port)");
        }
        return [trim($parts['host'], '[]'), (int) $parts['port']];
    }

    /** A SHA-256 fingerprint in hex, colons optional, as 32 bytes. */
    public static function parseFingerprint(string $text): string
    {
        $hex = str_replace(':', '', $text);
        if (strlen($hex) !== 64 || !ctype_xdigit($hex)) {
            throw new \InvalidArgumentException("fingerprint isn't a SHA-256 in hex");
        }
        return (string) hex2bin($hex);
    }

    /** Dials and waits (in this coroutine) for the handshake: false when it failed (see failure()). */
    public function open(): bool
    {
        try {
            $this->configure();
        } catch (\Throwable $error) {
            $this->fail($error->getMessage());
            return false;
        }
        $this->opened = new CoChannel(1);
        \OpenSwoole\Event::add($this->fd, function (): void {
            $this->tick();
        });
        $this->watching = true;
        $this->service();
        if ($this->phase === self::PHASE_CONNECTING) {
            $result = $this->opened->pop(10.0);
            if ($result === false && $this->phase === self::PHASE_CONNECTING) {
                $this->fail('quic handshake timed out');
            }
        }
        $this->opened = null;
        return $this->phase === self::PHASE_OPEN;
    }

    private function configure(): void
    {
        $ffi = $this->ffi;
        $servername = isset($this->quic['servername']) && $this->quic['servername'] !== '' ? (string) $this->quic['servername'] : null;
        $byIp = $servername === null && filter_var($this->host, FILTER_VALIDATE_IP) !== false;
        $checkName = $servername ?? $this->host;

        $this->ctx = $ffi->SSL_CTX_new($ffi->OSSL_QUIC_client_method());
        if (OpenSsl::isNull($this->ctx)) {
            throw new \RuntimeException($this->ossl->error('failed to create a quic context'));
        }
        if ($this->pin !== null) {
            // Pinning replaces the other checks: the fingerprint is compared after the handshake
            $ffi->SSL_CTX_set_verify($this->ctx, OpenSsl::VERIFY_NONE, null);
        } else {
            $ffi->SSL_CTX_set_verify($this->ctx, OpenSsl::VERIFY_PEER, null);
            $ca = $this->quic['ca'] ?? null;
            if (is_string($ca) && $ca !== '') {
                $certificates = $this->ossl->readCertificates($ca);
                if ($certificates === []) {
                    throw new \RuntimeException('ca has no PEM certificate');
                }
                $store = $ffi->SSL_CTX_get_cert_store($this->ctx);
                foreach ($certificates as $certificate) {
                    $ffi->X509_STORE_add_cert($store, $certificate);
                    $ffi->X509_free($certificate);
                }
                $ffi->ERR_clear_error();
            } elseif ($ffi->SSL_CTX_set_default_verify_paths($this->ctx) !== 1) {
                throw new \RuntimeException($this->ossl->error('failed to load the system certificates'));
            }
        }

        $this->ssl = $ffi->SSL_new($this->ctx);
        if (OpenSsl::isNull($this->ssl)) {
            throw new \RuntimeException($this->ossl->error('failed to create a quic connection'));
        }
        $name = $this->ossl->bytes($checkName);
        if (!$byIp && $ffi->SSL_ctrl($this->ssl, OpenSsl::CTRL_SET_TLSEXT_HOSTNAME, 0, $name) !== 1) {
            throw new \RuntimeException($this->ossl->error('invalid server name'));
        }
        if ($this->pin === null) {
            $set = $byIp
                ? $ffi->X509_VERIFY_PARAM_set1_ip_asc($ffi->SSL_get0_param($this->ssl), $checkName)
                : $ffi->SSL_set1_host($this->ssl, $checkName);
            if ($set !== 1) {
                throw new \RuntimeException($this->ossl->error('invalid server name'));
            }
        }
        $alpn = $this->ossl->alpn();
        if ($ffi->SSL_set_alpn_protos($this->ssl, $alpn, strlen(Frames::ALPN) + 1) !== 0
            || $ffi->SSL_set_value_uint($this->ssl, OpenSsl::VALUE_CLASS_FEATURE_REQUEST, OpenSsl::VALUE_QUIC_IDLE_TIMEOUT, OpenSsl::IDLE_TIMEOUT_MS) !== 1
            || $ffi->SSL_set_default_stream_mode($this->ssl, OpenSsl::DEFAULT_STREAM_MODE_NONE) !== 1
            || $ffi->SSL_set_incoming_stream_policy($this->ssl, OpenSsl::INCOMING_STREAM_POLICY_REJECT, 0) !== 1
            || $ffi->SSL_set_blocking_mode($this->ssl, 0) !== 1) {
            throw new \RuntimeException($this->ossl->error('failed to configure the quic connection'));
        }
        // A connected UDP socket of its own (one per connection, reconnects included)
        [$this->fd] = $this->ossl->udpSocket($this->ssl, $this->host, $this->port, false);
    }

    public function sendText(string $text): bool
    {
        if ($this->phase === self::PHASE_CLOSED) {
            return false;
        }
        $this->tx .= Frames::encode($text);
        $this->schedule();
        return true;
    }

    /** Writes $bytes as they are on the connection's stream (raw connections). */
    public function write(string $bytes): void
    {
        $this->tx .= $bytes;
        $this->service();
    }

    /** Opens one more stream and writes $bytes on it (raw connections). */
    public function writeOnNewStream(string $bytes, bool $unidirectional = false): int
    {
        $stream = $this->ffi->SSL_new_stream($this->ssl, OpenSsl::STREAM_FLAG_NO_BLOCK | ($unidirectional ? OpenSsl::STREAM_FLAG_UNI : 0));
        if (OpenSsl::isNull($stream)) {
            throw new \RuntimeException($this->ossl->error('failed to open a quic stream'));
        }
        $this->extraStreams[] = $stream;
        $this->ossl->flush($stream, $bytes);
        $this->service();
        return count($this->extraStreams) - 1;
    }

    /** Whether the server refused the stream writeOnNewStream opened. */
    public function extraStreamRefused(int $index): bool
    {
        $this->service();
        $stream = $this->extraStreams[$index];
        $sending = $this->ffi->SSL_get_stream_write_state($stream);
        return $sending === OpenSsl::STREAM_STATE_RESET_LOCAL
            || $sending === OpenSsl::STREAM_STATE_RESET_REMOTE
            || $sending === OpenSsl::STREAM_STATE_CONN_CLOSED
            || $this->ffi->SSL_get_stream_read_state($stream) === OpenSsl::STREAM_STATE_RESET_REMOTE;
    }

    public function isOpen(): bool
    {
        return $this->phase === self::PHASE_OPEN;
    }

    public function receive(): ?string
    {
        while (true) {
            if ($this->inbox !== []) {
                return array_shift($this->inbox);
            }
            if ($this->protocolError !== null) {
                $error = $this->protocolError;
                $this->protocolError = null;
                throw new ClientLinkError($error);
            }
            if ($this->phase === self::PHASE_CLOSED) {
                return null;
            }
            $this->waiter = new CoChannel(1);
            $this->waiter->pop();
            $this->waiter = null;
        }
    }

    /** Waits up to $timeout seconds for the next message (tests): null on timeout or close. */
    public function receiveWithin(float $timeout): ?string
    {
        if ($this->inbox === [] && $this->phase !== self::PHASE_CLOSED) {
            $this->waiter = new CoChannel(1);
            $this->waiter->pop($timeout);
            $this->waiter = null;
        }
        return $this->inbox === [] ? null : array_shift($this->inbox);
    }

    public function close(): void
    {
        $this->closeNow();
    }

    private function wake(): void
    {
        if ($this->waiter !== null && $this->waiter->isEmpty()) {
            $this->waiter->push(true);
        }
    }

    /** Runs service() once the current work is done, so everything sent meanwhile goes out together. */
    private function schedule(): void
    {
        if ($this->deferred) {
            return;
        }
        $this->deferred = true;
        \OpenSwoole\Event::defer(function (): void {
            $this->deferred = false;
            $this->tick();
        });
    }

    /** From the reactor (socket readable, timer, deferred send): service() in a coroutine of its own. */
    private function tick(): void
    {
        if ($this->phase === self::PHASE_CLOSED) {
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
        if ($this->phase === self::PHASE_CLOSED) {
            return;
        }
        if ($this->servicing) {
            $this->again = true;
            return;
        }
        $this->servicing = true;
        try {
            do {
                $this->again = false;
                $this->ffi->SSL_handle_events($this->ssl);
                if ($this->phase === self::PHASE_CONNECTING) {
                    $this->handshake();
                }
                if ($this->phase === self::PHASE_OPEN) {
                    $this->pump();
                }
            } while ($this->again && $this->phase !== self::PHASE_CLOSED);
        } finally {
            $this->servicing = false;
        }
        $this->arm();
    }

    private function handshake(): void
    {
        $result = $this->ffi->SSL_connect($this->ssl);
        if ($result === 1) {
            $this->openStream();
            return;
        }
        $code = $this->ffi->SSL_get_error($this->ssl, $result);
        if ($code === OpenSsl::ERROR_WANT_READ || $code === OpenSsl::ERROR_WANT_WRITE) {
            $this->ffi->ERR_clear_error();
            return;
        }
        $verify = $this->ffi->SSL_get_verify_result($this->ssl);
        if ($verify !== 0) {
            $this->ffi->ERR_clear_error();
            $this->fail('certificate verify failed: ' . $this->ffi->X509_verify_cert_error_string($verify));
        } else {
            $this->fail($this->ossl->error('quic handshake failed'));
        }
    }

    private function pinMatches(): bool
    {
        $certificate = $this->ffi->SSL_get0_peer_certificate($this->ssl);
        if (OpenSsl::isNull($certificate)) {
            return false;
        }
        $digest = $this->ffi->new('unsigned char[64]');
        $length = $this->ffi->new('unsigned int');
        if ($this->ffi->X509_digest($certificate, $this->ffi->EVP_sha256(), $digest, FFI::addr($length)) !== 1 || $length->cdata !== 32) {
            return false;
        }
        return hash_equals((string) $this->pin, FFI::string($digest, 32));
    }

    private function openStream(): void
    {
        if ($this->pin !== null && !$this->pinMatches()) {
            $this->fail('certificate fingerprint mismatch');
            return;
        }
        $this->stream = $this->ffi->SSL_new_stream($this->ssl, OpenSsl::STREAM_FLAG_NO_BLOCK);
        if (OpenSsl::isNull($this->stream)) {
            $this->fail($this->ossl->error('failed to open a quic stream'));
            return;
        }
        $this->ffi->SSL_ctrl($this->stream, OpenSsl::CTRL_MODE, OpenSsl::MODE_ENABLE_PARTIAL_WRITE | OpenSsl::MODE_ACCEPT_MOVING_WRITE_BUFFER, null);
        // The hello goes in front of anything queued while connecting
        if (!$this->raw) {
            $this->tx = Frames::hello($this->headers) . $this->tx;
        }
        $this->phase = self::PHASE_OPEN;
        $this->lastSent = microtime(true);
        $this->opened?->push(true);
    }

    private function pump(): void
    {
        for ($iteration = 0; $iteration < 32 && $this->phase === self::PHASE_OPEN; $iteration++) {
            if (!$this->raw && $this->tx === '' && (microtime(true) - $this->lastSent) * 1000 >= OpenSsl::KEEPALIVE_MS) {
                $this->tx = "\0";
            }
            $pending = strlen($this->tx);
            if ($pending > 0) {
                if (!$this->ossl->flush($this->stream, $this->tx)) {
                    $this->closeNow();
                    return;
                }
                if (strlen($this->tx) !== $pending) {
                    $this->lastSent = microtime(true);
                }
            }
            $data = $this->ossl->read($this->stream, $this->chunk, 65536);
            if ($data === null) {
                $this->closeNow();
                return;
            }
            if ($data === '') {
                if (strlen($this->tx) === $pending || $this->tx === '') {
                    return;
                }
                continue;
            }
            try {
                $frames = $this->reader->push($data);
            } catch (FrameTooLarge) {
                $this->protocolFailure('message too big');
                return;
            }
            foreach ($frames as $frame) {
                $this->inbox[] = $frame;
            }
            $this->wake();
            if ($this->reader->failed()) {
                $this->protocolFailure('message too big');
                return;
            }
        }
    }

    private function arm(): void
    {
        if ($this->phase === self::PHASE_CLOSED || $this->ssl === null) {
            return;
        }
        $timeout = $this->ossl->eventTimeoutMs($this->ssl);
        if ($this->phase === self::PHASE_OPEN && !$this->raw) {
            $keepalive = (int) (OpenSsl::KEEPALIVE_MS - (microtime(true) - $this->lastSent) * 1000);
            $timeout = $timeout === null ? $keepalive : min($timeout, $keepalive);
        }
        if ($this->ffi->SSL_net_write_desired($this->ssl) === 1) {
            $timeout = 1;
        }
        if ($timeout === null) {
            return;
        }
        $timeout = max(1, $timeout);
        $deadline = microtime(true) + $timeout / 1000;
        if (is_int($this->timer) && $this->timerDeadline <= $deadline + 0.0005) {
            return;
        }
        if (is_int($this->timer)) {
            \OpenSwoole\Timer::clear($this->timer);
        }
        $this->timerDeadline = $deadline;
        $this->timer = \OpenSwoole\Timer::after($timeout, function (): void {
            $this->timer = null;
            $this->tick();
        });
    }

    private function protocolFailure(string $message): void
    {
        $this->protocolError = $message;
        $this->closeNow();
    }

    private function fail(string $message): void
    {
        $this->failure ??= $message;
        $this->closeNow();
        $this->opened?->push(false);
    }

    /** Sends CONNECTION_CLOSE right away, without waiting for the peer, and frees the QUIC objects. */
    private function closeNow(): void
    {
        if ($this->phase === self::PHASE_CLOSED) {
            return;
        }
        $this->phase = self::PHASE_CLOSED;
        // Before OpenSSL closes the socket
        if ($this->watching) {
            \OpenSwoole\Event::del($this->fd);
            $this->watching = false;
        }
        if (is_int($this->timer)) {
            \OpenSwoole\Timer::clear($this->timer);
            $this->timer = null;
        }
        $ffi = $this->ffi;
        if ($this->stream !== null) {
            $this->ossl->flush($this->stream, $this->tx);
            $ffi->SSL_stream_conclude($this->stream, 0);
        }
        if ($this->ssl !== null && !OpenSsl::isNull($this->ssl)) {
            // Queues CONNECTION_CLOSE; the next round of events sends it. The draining period isn't waited for.
            $ffi->SSL_shutdown_ex($this->ssl, OpenSsl::SHUTDOWN_FLAG_RAPID | OpenSsl::SHUTDOWN_FLAG_NO_BLOCK, null, 0);
            $ffi->SSL_handle_events($this->ssl);
        }
        $ffi->ERR_clear_error();
        foreach ($this->extraStreams as $stream) {
            $ffi->SSL_free($stream);
        }
        $this->extraStreams = [];
        if ($this->stream !== null) {
            $ffi->SSL_free($this->stream);
            $this->stream = null;
        }
        if ($this->ssl !== null && !OpenSsl::isNull($this->ssl)) {
            $ffi->SSL_free($this->ssl);
        } elseif ($this->fd >= 0) {
            $ffi->BIO_closesocket($this->fd);
        }
        $this->ssl = null;
        if ($this->ctx !== null && !OpenSsl::isNull($this->ctx)) {
            $ffi->SSL_CTX_free($this->ctx);
        }
        $this->ctx = null;
        $this->tx = '';
        $this->wake();
    }
}
