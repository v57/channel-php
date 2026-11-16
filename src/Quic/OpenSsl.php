<?php
declare(strict_types=1);

namespace ChannelPhp\Quic;

use FFI;
use FFI\CData;

/**
 * PHP FFI bindings to OpenSSL 3.5's native QUIC (the API channel-c and channel-zig use). Loaded on first use, so
 * WebSocket-only programs never need ext-ffi or OpenSSL 3.5.
 */
final class OpenSsl
{
    // From OpenSSL 3.5's headers
    public const ERROR_WANT_READ = 2;
    public const ERROR_WANT_WRITE = 3;
    public const VERIFY_NONE = 0;
    public const VERIFY_PEER = 1;
    public const STREAM_FLAG_UNI = 1;
    public const STREAM_FLAG_NO_BLOCK = 2;
    public const ACCEPT_STREAM_NO_BLOCK = 1;
    public const ACCEPT_CONNECTION_NO_BLOCK = 1;
    public const MODE_ENABLE_PARTIAL_WRITE = 1;
    public const MODE_ACCEPT_MOVING_WRITE_BUFFER = 2;
    public const CTRL_MODE = 33;
    public const CTRL_SET_TLSEXT_HOSTNAME = 55;
    public const CTRL_CHAIN_CERT = 89;
    public const DEFAULT_STREAM_MODE_NONE = 0;
    public const INCOMING_STREAM_POLICY_ACCEPT = 1;
    public const INCOMING_STREAM_POLICY_REJECT = 2;
    public const VALUE_CLASS_FEATURE_REQUEST = 1;
    public const VALUE_QUIC_IDLE_TIMEOUT = 5;
    public const SHUTDOWN_FLAG_RAPID = 1;
    public const SHUTDOWN_FLAG_NO_BLOCK = 4;
    public const STREAM_TYPE_BIDI = 3;
    public const STREAM_STATE_RESET_LOCAL = 4;
    public const STREAM_STATE_RESET_REMOTE = 5;
    public const STREAM_STATE_CONN_CLOSED = 6;
    public const TLSEXT_ERR_OK = 0;
    public const TLSEXT_ERR_ALERT_FATAL = 2;
    public const NPN_NEGOTIATED = 1;
    public const BIO_CLOSE = 1;
    public const LOOKUP_CLIENT = 0;
    public const LOOKUP_SERVER = 1;
    public const SOCK_REUSEADDR = 1;
    public const SOCK_INFO_ADDRESS = 0;
    public const AF_UNSPEC = 0;
    public const AF_INET = 2;
    public const SOCK_DGRAM = 2;
    public const IPPROTO_UDP = 17;

    /** The QUIC idle timeout both sides set, and the client's keepalive interval. */
    public const IDLE_TIMEOUT_MS = 30000;
    public const KEEPALIVE_MS = 15000;

    private const CDEF = <<<'C'
        typedef int (*alpn_select_cb)(void *ssl, void *out, void *outlen, const void *in, unsigned int inlen,
                                      void *arg);
        struct timeval_ { long tv_sec; long tv_usec; };

        unsigned long OpenSSL_version_num(void);
        const void *OSSL_QUIC_client_method(void);
        const void *OSSL_QUIC_server_method(void);
        void *SSL_CTX_new(const void *method);
        void SSL_CTX_free(void *ctx);
        void SSL_CTX_set_verify(void *ctx, int mode, void *callback);
        void *SSL_CTX_get_cert_store(const void *ctx);
        int SSL_CTX_set_default_verify_paths(void *ctx);
        int SSL_CTX_use_certificate(void *ctx, void *x509);
        int SSL_CTX_use_PrivateKey(void *ctx, void *pkey);
        int SSL_CTX_check_private_key(const void *ctx);
        long SSL_CTX_ctrl(void *ctx, int cmd, long larg, void *parg);
        void SSL_CTX_set_alpn_select_cb(void *ctx, alpn_select_cb cb, void *arg);
        int SSL_select_next_proto(void *out, void *outlen, const void *server, unsigned int server_len,
                                  const void *client, unsigned int client_len);
        void *SSL_new(void *ctx);
        void SSL_free(void *ssl);
        long SSL_ctrl(void *ssl, int cmd, long larg, void *parg);
        int SSL_set1_host(void *ssl, const char *host);
        void *SSL_get0_param(void *ssl);
        int SSL_set_alpn_protos(void *ssl, const void *protos, unsigned int protos_len);
        int SSL_set_value_uint(void *ssl, uint32_t class_, uint32_t id, uint64_t v);
        int SSL_set_default_stream_mode(void *ssl, uint32_t mode);
        int SSL_set_incoming_stream_policy(void *ssl, int policy, uint64_t aec);
        int SSL_set_blocking_mode(void *ssl, int blocking);
        void SSL_set_bio(void *ssl, void *rbio, void *wbio);
        int SSL_set1_initial_peer_addr(void *ssl, const void *peer_addr);
        int SSL_connect(void *ssl);
        int SSL_get_error(const void *ssl, int ret);
        long SSL_get_verify_result(const void *ssl);
        void *SSL_get0_peer_certificate(const void *ssl);
        void *SSL_new_stream(void *ssl, uint64_t flags);
        int SSL_write_ex(void *ssl, const void *buf, size_t num, size_t *written);
        int SSL_read_ex(void *ssl, void *buf, size_t num, size_t *readbytes);
        int SSL_handle_events(void *ssl);
        int SSL_get_event_timeout(void *ssl, struct timeval_ *tv, int *is_infinite);
        int SSL_net_write_desired(void *ssl);
        int SSL_shutdown_ex(void *ssl, uint64_t flags, const void *args, size_t args_len);
        int SSL_stream_conclude(void *ssl, uint64_t flags);
        int SSL_stream_reset(void *ssl, const void *args, size_t args_len);
        void *SSL_accept_stream(void *ssl, uint64_t flags);
        int SSL_get_stream_type(void *ssl);
        int SSL_get_stream_read_state(void *ssl);
        int SSL_get_stream_write_state(void *ssl);
        int SSL_get_conn_close_info(void *ssl, void *info, size_t info_len);
        void *SSL_new_listener(void *ctx, uint64_t flags);
        int SSL_listen(void *ssl);
        void *SSL_accept_connection(void *ssl, uint64_t flags);

        void ERR_clear_error(void);
        unsigned long ERR_peek_last_error(void);
        void ERR_error_string_n(unsigned long e, char *buf, size_t len);
        void *BIO_new_mem_buf(const void *buf, int len);
        int BIO_free(void *bio);
        void *PEM_read_bio_X509(void *bio, void *x, void *cb, void *u);
        void *PEM_read_bio_PrivateKey(void *bio, void *x, void *cb, void *u);
        void X509_free(void *x509);
        void EVP_PKEY_free(void *pkey);
        int X509_STORE_add_cert(void *store, void *x509);
        const void *EVP_sha256(void);
        int X509_digest(const void *x509, const void *type, void *md, void *len);
        const char *X509_verify_cert_error_string(long n);
        int X509_VERIFY_PARAM_set1_ip_asc(void *param, const char *ipasc);
        int BIO_lookup_ex(const char *host, const char *service, int lookup_type, int family, int socktype,
                          int protocol, void **res);
        const void *BIO_ADDRINFO_next(const void *bai);
        int BIO_ADDRINFO_family(const void *bai);
        const void *BIO_ADDRINFO_address(const void *bai);
        void BIO_ADDRINFO_free(void *bai);
        int BIO_socket(int domain, int socktype, int protocol, int options);
        int BIO_connect(int sock, const void *addr, int options);
        int BIO_bind(int sock, const void *addr, int options);
        int BIO_socket_nbio(int sock, int mode);
        int BIO_closesocket(int sock);
        void *BIO_new_dgram(int fd, int close_flag);
        void *BIO_ADDR_new(void);
        void BIO_ADDR_free(void *addr);
        int BIO_sock_info(int sock, int type, void **info);
        unsigned short BIO_ADDR_rawport(const void *addr);
        C;

    private static ?self $instance = null;

    public readonly FFI $ffi;

    private function __construct()
    {
        if (!extension_loaded('ffi')) {
            throw new QuicUnavailable('QUIC needs ext-ffi (and ffi.enable=preload in the CLI, or true)');
        }
        $errors = [];
        $ffi = null;
        foreach (self::candidates() as $library) {
            try {
                // libssl's handle also resolves libcrypto's symbols (its dependency)
                $ffi = FFI::cdef(self::CDEF, $library);
                break;
            } catch (\Throwable $error) {
                $errors[] = $library . ': ' . $error->getMessage();
            }
        }
        if ($ffi === null) {
            throw new QuicUnavailable('OpenSSL 3.5+ not found (set CHANNEL_QUIC_LIBSSL). ' . implode('; ', $errors));
        }
        $version = (int) $ffi->OpenSSL_version_num();
        // 0xMNN00PP0L: QUIC servers (SSL_new_listener) need 3.5
        if ($version < 0x30500000) {
            throw new QuicUnavailable(sprintf('OpenSSL %d.%d is too old: QUIC needs 3.5+', ($version >> 28) & 0xf, ($version >> 20) & 0xff));
        }
        $this->ffi = $ffi;
    }

    public static function instance(): self
    {
        return self::$instance ??= new self();
    }

    /** @return list<string> */
    private static function candidates(): array
    {
        $override = getenv('CHANNEL_QUIC_LIBSSL');
        $names = is_string($override) && $override !== '' ? [$override] : [];
        if (PHP_OS_FAMILY === 'Darwin') {
            return [...$names, '/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib', '/usr/local/opt/openssl@3/lib/libssl.3.dylib', 'libssl.3.dylib'];
        }
        return [...$names, 'libssl.so.3', 'libssl.so'];
    }

    /** The last OpenSSL error as text after $fallback, or $fallback. Clears the error queue. */
    public function error(string $fallback): string
    {
        $code = $this->ffi->ERR_peek_last_error();
        if ($code === 0) {
            return $fallback;
        }
        $buffer = $this->ffi->new('char[256]');
        $this->ffi->ERR_error_string_n($code, $buffer, 256);
        $this->ffi->ERR_clear_error();
        return $fallback . ': ' . FFI::string($buffer);
    }

    public static function isNull(?CData $pointer): bool
    {
        return $pointer === null || FFI::isNull($pointer);
    }

    /** A C copy of $bytes, NUL-terminated (keep the returned CData alive while C uses it). */
    public function bytes(string $bytes): CData
    {
        $length = strlen($bytes);
        $buffer = $this->ffi->new('char[' . ($length + 1) . ']');
        if ($length > 0) {
            FFI::memcpy($buffer, $bytes, $length);
        }
        $buffer[$length] = "\0";
        return $buffer;
    }

    /** @return list<CData> every PEM certificate in $pem, in order (free them with X509_free) */
    public function readCertificates(string $pem): array
    {
        $text = $this->bytes($pem);
        $bio = $this->ffi->BIO_new_mem_buf($text, -1);
        $certificates = [];
        while (!self::isNull($certificate = $this->ffi->PEM_read_bio_X509($bio, null, null, null))) {
            $certificates[] = $certificate;
        }
        $this->ffi->BIO_free($bio);
        $this->ffi->ERR_clear_error();
        return $certificates;
    }

    public function readPrivateKey(string $pem): ?CData
    {
        $text = $this->bytes($pem);
        $bio = $this->ffi->BIO_new_mem_buf($text, -1);
        $key = $this->ffi->PEM_read_bio_PrivateKey($bio, null, null, null);
        $this->ffi->BIO_free($bio);
        $this->ffi->ERR_clear_error();
        return self::isNull($key) ? null : $key;
    }

    /** Milliseconds until $ssl needs SSL_handle_events again (null: no timer). */
    public function eventTimeoutMs(CData $ssl): ?int
    {
        $tv = $this->ffi->new('struct timeval_');
        $infinite = $this->ffi->new('int');
        if ($this->ffi->SSL_get_event_timeout($ssl, FFI::addr($tv), FFI::addr($infinite)) !== 1 || $infinite->cdata !== 0) {
            return null;
        }
        // tv_usec is 32-bit on macOS (then padding): its low 31 bits hold any valid value
        return $tv->tv_sec * 1000 + intdiv(($tv->tv_usec & 0x7fffffff) + 999, 1000);
    }

    /** Writes as much of $queue to $stream as it takes. False on a stream error. */
    public function flush(CData $stream, string &$queue): bool
    {
        $written = $this->ffi->new('size_t');
        while ($queue !== '') {
            $length = min(strlen($queue), 262144);
            $buffer = $this->ffi->new("char[$length]");
            FFI::memcpy($buffer, $queue, $length);
            if ($this->ffi->SSL_write_ex($stream, $buffer, $length, FFI::addr($written)) === 1) {
                $queue = (string) substr($queue, $written->cdata);
                continue;
            }
            $code = $this->ffi->SSL_get_error($stream, 0);
            $this->ffi->ERR_clear_error();
            return $code === self::ERROR_WANT_READ || $code === self::ERROR_WANT_WRITE;
        }
        return true;
    }

    /** What $stream has: null when it ended, was reset or the connection closed, otherwise the bytes (maybe ''). */
    public function read(CData $stream, CData $chunk, int $chunkSize): ?string
    {
        $count = $this->ffi->new('size_t');
        $data = '';
        while (true) {
            if ($this->ffi->SSL_read_ex($stream, $chunk, $chunkSize, FFI::addr($count)) === 1) {
                $data .= FFI::string($chunk, $count->cdata);
                continue;
            }
            $code = $this->ffi->SSL_get_error($stream, 0);
            $this->ffi->ERR_clear_error();
            if ($code === self::ERROR_WANT_READ || $code === self::ERROR_WANT_WRITE) {
                return $data;
            }
            // Stream finished or reset, or the connection closed: what came before still counts
            return $data === '' ? null : $data;
        }
    }

    /** `\x09channel/1`, ALPN's wire form. */
    public function alpn(): CData
    {
        return $this->bytes(chr(strlen(Frames::ALPN)) . Frames::ALPN);
    }

    /**
     * A non-blocking UDP socket on $host:$port, connected ($server false) or bound with SO_REUSEADDR, as a
     * datagram BIO set on $ssl. Returns the socket and the bound port.
     *
     * @return array{0:int, 1:int}
     */
    public function udpSocket(CData $ssl, string $host, int $port, bool $server): array
    {
        $results = $this->ffi->new('void*');
        if ($this->ffi->BIO_lookup_ex($host, (string) $port, $server ? self::LOOKUP_SERVER : self::LOOKUP_CLIENT, self::AF_UNSPEC, self::SOCK_DGRAM, self::IPPROTO_UDP, FFI::addr($results)) !== 1) {
            throw new \RuntimeException($this->error($server ? 'failed to resolve the listen address' : 'failed to resolve the quic host'));
        }
        try {
            $chosen = $results;
            if (!$server) {
                // IPv4 first: listeners on 0.0.0.0 don't answer on ::1
                for ($info = $results; !self::isNull($info); $info = $this->ffi->BIO_ADDRINFO_next($info)) {
                    if ($this->ffi->BIO_ADDRINFO_family($info) === self::AF_INET) {
                        $chosen = $info;
                        break;
                    }
                }
            }
            $address = $this->ffi->BIO_ADDRINFO_address($chosen);
            $fd = $this->ffi->BIO_socket($this->ffi->BIO_ADDRINFO_family($chosen), self::SOCK_DGRAM, self::IPPROTO_UDP, 0);
            $ok = $fd >= 0 && ($server
                ? $this->ffi->BIO_bind($fd, $address, self::SOCK_REUSEADDR) === 1
                : $this->ffi->BIO_connect($fd, $address, 0) === 1) && $this->ffi->BIO_socket_nbio($fd, 1) === 1;
            if (!$ok) {
                if ($fd >= 0) {
                    $this->ffi->BIO_closesocket($fd);
                }
                $this->ffi->ERR_clear_error();
                throw new \RuntimeException($server ? "failed to bind the quic server address $host:$port" : 'failed to open a udp socket');
            }
            $bound = $port;
            if ($server) {
                $local = $this->ffi->BIO_ADDR_new();
                // union BIO_sock_info_u { BIO_ADDR *addr; }
                $info = $this->ffi->new('void*[1]');
                $info[0] = $local;
                if ($this->ffi->BIO_sock_info($fd, self::SOCK_INFO_ADDRESS, $info) === 1) {
                    $raw = $this->ffi->BIO_ADDR_rawport($local);
                    $bound = (($raw & 0xff) << 8) | ($raw >> 8); // network byte order
                }
                $this->ffi->BIO_ADDR_free($local);
            }
            $bio = $this->ffi->BIO_new_dgram($fd, self::BIO_CLOSE);
            if (self::isNull($bio)) {
                $this->ffi->BIO_closesocket($fd);
                throw new \RuntimeException($this->error('failed to open a udp socket'));
            }
            $this->ffi->SSL_set_bio($ssl, $bio, $bio);
            if (!$server && $this->ffi->SSL_set1_initial_peer_addr($ssl, $address) !== 1) {
                throw new \RuntimeException($this->error('failed to set the quic peer address'));
            }
            return [$fd, $bound];
        } finally {
            $this->ffi->BIO_ADDRINFO_free($results);
        }
    }
}
