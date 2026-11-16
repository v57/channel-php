<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. PHP implementation of Channel: run a websocket server, a websocket client, or communicate inside your process.

Also QUIC, experimental: a client and a server on OpenSSL 3.5's native QUIC through `ext-ffi` (see below).

# Installation (macOS/Homebrew)

```bash
brew install php@8.4 composer bun lcov autoconf pkg-config
pecl install openswoole
pecl install simdjson
pecl install pcov
```

# Dependencies

- [OpenSwoole](https://openswoole.com) (`ext-openswoole`, required): coroutines, the WebSocket server and the client
  sockets.
- [simdjson](https://pecl.php.net/package/simdjson) (`ext-simdjson`, optional): faster JSON decoding when installed;
  `ext-json` otherwise.
- `ext-ffi` (optional): the QUIC transport only, with `libssl`/`libcrypto` 3.5 or later at run time (Debian 13,
  Ubuntu 25.04+, Homebrew's `openssl@3`). Nothing loads it unless a program uses `quic://` or `listenQuic`.
- `bun`, `lcov` and `pcov` above are for the interop tests and coverage only.

```bash
composer install
```

# Usage

```php
<?php
declare(strict_types=1);

use ChannelPhp\Channel;

// Client
$client = (new Channel())->connect('ws://127.0.0.1:2049');
$response = $client->send('hello');
$client->stop();
```

```php
<?php
declare(strict_types=1);

use ChannelPhp\Channel;

// Server
$server = (new Channel())
    ->post('hello', static fn(): string => 'world')
    ->listen(2050, [
        'state' => static fn(array $headers): object => new stdClass(),
    ]);

// ... run clients ...
$server->stop();
```

Note: `listen()` uses `pcntl_fork` and should be called outside OpenSwoole coroutine context.

# QUIC (experimental)

The protocol over one QUIC stream, as in [channel/docs/quic.md](../channel/docs/quic.md): ALPN `channel/1`, TLS
always on, the client's headers in a first `hello` frame. Same requests, streams, events and reconnects as the
WebSocket client.

```php
<?php
use ChannelPhp\Channel;
use OpenSwoole\Coroutine;

Coroutine::run(static function (): void {
    // Server: QUIC has no plaintext mode, so it needs a certificate and key (PEM)
    $server = (new Channel())
        ->post('hello', static fn(): string => 'world')
        ->listenQuic(4433, [
            'cert' => file_get_contents('cert.pem'),
            'key' => file_get_contents('key.pem'),
            'state' => static fn(array $headers): object => (object) ['headers' => $headers],
        ]);

    // Client
    $client = (new Channel())->connect('quic://127.0.0.1:4433', [
        'quic' => ['ca' => file_get_contents('cert.pem')], // or 'fingerprint' => 'DD:D2:…', 'servername' => 'localhost'
    ]);
    echo $client->send('hello'), "\n";

    $client->stop();
    $server->stop();
});
```

- `quic` options: `ca` trusts a self-signed certificate (the address must be in its names), `fingerprint` pins the
  certificate's SHA-256 instead of verifying it, `servername` overrides the name sent and checked. Without them the
  system's roots verify the server. A mistake in them, or a missing `ext-ffi`/OpenSSL 3.5, throws from `connect()`.
- `listenQuic()` takes a port or `'host:port'` (127.0.0.1 by default) and the same `state` (built from the hello's
  headers, names lowercased), `onConnect`, `onDisconnect` and `maxPayload` options as `listen()`. Unlike `listen()`
  it runs in this process on OpenSwoole's reactor, so call it inside a coroutine (`Coroutine::run`, a server's
  worker) and `stop()` it there. `port()` gives the bound port, `connectionCount()` the open connections.
- Each connection dials from its own UDP socket. OpenSSL owns the socket; OpenSwoole's reactor watches it and a timer
  covers OpenSSL's timeouts and the client's 15 s keepalive, so nothing polls.
- OpenSSL can't lower the stream limits: the server resets any stream after a connection's first one.
- Set `CHANNEL_QUIC_LIBSSL` to load OpenSSL from another path; `ffi.enable` must allow `FFI::cdef` (the CLI default
  `preload` does). Without them QUIC throws `ChannelPhp\Quic\QuicUnavailable`.

# Process transport (in-memory)

```php
<?php
declare(strict_types=1);

use ChannelPhp\Channel;

$sender = (new Channel())
    ->post('hello', static fn(): string => 'world')
    ->process();

$value = $sender->send('hello');
$sender->stop();
```

# Requirements

- PHP `8.3+`
- Composer
- Bun (for interop tests/benchmark fixture)
- `ext-openswoole`
- `ext-simdjson` (optional, fast JSON decode path)
- `ext-ffi` and OpenSSL 3.5+ (optional, the QUIC transport)
- `ext-pcov` (for coverage script)

# Tool Versions (this environment)

- PHP: `8.3.30`
- Composer: `2.9.5`
- Bun: `1.3.8`
- lcov: `2.4-0`

# Scripts

- Run all tests:

```bash
./scripts/test
```

- Run benchmark (single server process + single client process):

```bash
./scripts/benchmark
```

This benchmark runs 4 stages, each for 5 seconds, with:
- `1` client at `1000` req/s
- `10` clients at `1000` req/s each
- `100` clients at `1000` req/s each
- `1000` clients at `1000` req/s each

Implementation follows `../channel/benchmark.md` stage lifecycle:
- random port per stage
- one server process + one client process per stage
- wait for JSON `ready`
- sleep measured window
- sample process usage via `ps`
- send `SIGTERM`
- compute `rps = requests / 5s`

It writes `benchmark.json` with:
- `language`
- `results[]` where each row has:
  - `rps`
  - `clients`
  - `client.cpu`, `client.mem`
  - `server.cpu`, `server.mem`

Optional env overrides:
- `BENCH_STAGE_MS` (default `5000`)
- `BENCH_RPS_PER_CLIENT` (default `1000`)

- Run coverage with `pcov`, convert Clover -> LCOV:

```bash
./scripts/coverage
```

Coverage output:
- `coverage/clover.xml`
- `coverage/lcov.info`

# JSON Codec

`ChannelPhp\Internal\JsonCodec` uses:
- `simdjson_decode()` when `ext-simdjson` is available
- fallback to `json_decode(..., JSON_THROW_ON_ERROR)` otherwise

Encoding uses `json_encode` with throw-on-error flags.

# Test Suites

- `tests/Unit`: core channel/send/values semantics
- `tests/Interop`: PHP client against `../channel/tests/server.ts`
- `tests/E2E`: PHP client <-> PHP server transport behavior
- `tests/Quic`: the QUIC transport: frames, PHP client <-> PHP server, the wire rules with raw frames, the PHP client
  against the TypeScript fixture's QUIC server (its own fixture on 2449, `CHANNEL_QUIC_TEST_PORT`), and the TypeScript
  reference's QUIC client against the PHP server. Skipped without `ext-ffi` and OpenSSL 3.5
