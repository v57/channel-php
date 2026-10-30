<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. PHP implementation of Channel: run a websocket server, a websocket client, or communicate inside your process.

WebSocket only: PHP has no usable QUIC library. Languages with QUIC: TypeScript, Go, Swift, C#, Rust, Python, Elixir,
Java, Kotlin, C and Zig (`channel/docs/quic.md`).

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
