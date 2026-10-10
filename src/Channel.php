<?php
declare(strict_types=1);

namespace ChannelPhp;

use ChannelPhp\Transports\ClientTransport;
use ChannelPhp\Transports\ProcessTransport;
use ChannelPhp\Transports\ServerTransport;
use OpenSwoole\Coroutine;
use Traversable;

final class Channel
{
    private int $id = 0;

    /** @var array<int, PendingRequest> */
    public array $requests = [];

    /** @var array<string, callable> */
    public array $postApi = [];

    /** @var array<int, array{path: callable, request: callable}> */
    public array $otherPostApi = [];

    /** @var array<string, callable> */
    public array $streamApi = [];

    /** @var array<int, array{path: callable, request: callable}> */
    public array $otherStreamApi = [];

    private $onDisconnect = null;

    /** @var array<string, Subscription>|null */
    public ?array $eventsApi = null;

    public function post(string $path, callable $request): self
    {
        $this->postApi[$path] = $request;
        return $this;
    }

    public function postOther(callable $path, callable $request): self
    {
        $this->otherPostApi[] = ['path' => $path, 'request' => $request];
        return $this;
    }

    public function stream(string $path, callable $request): self
    {
        $this->streamApi[$path] = $request;
        return $this;
    }

    public function streamOther(callable $path, callable $request): self
    {
        $this->otherStreamApi[] = ['path' => $path, 'request' => $request];
        return $this;
    }

    public function onDisconnect(callable $action): self
    {
        $this->onDisconnect = $action;
        return $this;
    }

    public function disconnect(mixed $state, Sender $sender): void
    {
        try {
            if ($this->onDisconnect !== null) {
                ($this->onDisconnect)($state, $sender);
            }
        } catch (\Throwable) {
        }

        foreach ($sender->streams as $stream) {
            try {
                if (is_object($stream) && method_exists($stream, 'return')) {
                    $stream->return();
                }
            } catch (\Throwable) {
            }
        }

        foreach ($sender->requests as $request) {
            $request->isCancelled = true;
            foreach ($request->cancelActions as $action) {
                try {
                    $action();
                } catch (\Throwable) {
                }
            }
        }

        $this->failRequests($sender, 'disconnected');

        $sender->streams = [];
        $sender->requests = [];
    }

    /**
     * Settles every pending outgoing request made through $sender with $error.
     * (TS silently drops them on disconnect, which strands the caller's waiter forever.)
     */
    public function failRequests(Sender $sender, string $error): void
    {
        $failed = [];
        foreach ($this->requests as $id => $request) {
            if ($request->sender === $sender) {
                $failed[$id] = $request;
            }
        }

        foreach ($failed as $id => $request) {
            unset($this->requests[$id]);
            try {
                ($request->response)(['id' => $id, 'error' => $error]);
            } catch (\Throwable) {
            }
        }
    }

    public function makeRequest(
        string $path,
        mixed $body,
        mixed $context,
        callable $response,
        ?Sender $sender = null,
        bool $cancellable = true,
    ): array {
        $id = $this->id++;
        $request = ['id' => $id, 'path' => $path, 'body' => $body, 'context' => $context];
        if (!$cancellable) {
            $request['cancellable'] = false;
        }
        $this->requests[$id] = new PendingRequest($request, $response, $sender);

        return $request;
    }

    public function makeStream(
        string $stream,
        mixed $body,
        mixed $context,
        callable $response,
        ?Sender $sender = null,
    ): array {
        $id = $this->id++;
        $request = ['id' => $id, 'stream' => $stream, 'body' => $body, 'context' => $context];
        $this->requests[$id] = new PendingRequest($request, $response, $sender);

        return $request;
    }

    public function makeSubscription(
        string $sub,
        mixed $body,
        mixed $context,
        callable $response,
        ?Sender $sender = null,
    ): array {
        $id = $this->id++;
        $request = ['id' => $id, 'sub' => $sub, 'body' => $body, 'context' => $context];
        $this->requests[$id] = new PendingRequest($request, $response, $sender);

        return $request;
    }

    public function cancel(int $id): void
    {
        unset($this->requests[$id]);
    }

    public function receive(mixed $some, Controller $controller): void
    {
        if (is_array($some) && array_is_list($some)) {
            foreach ($some as $item) {
                if (is_array($item)) {
                    $this->receiveOne($item, $controller);
                }
            }
            return;
        }

        if (is_array($some)) {
            $this->receiveOne($some, $controller);
        }
    }

    public function receiveOne(array $some, Controller $controller): void
    {
        if (!empty($some['path'])) {
            $this->handlePath($some, $controller);
            return;
        }

        if (!empty($some['stream'])) {
            $this->handleStream($some, $controller);
            return;
        }

        if (array_key_exists('cancel', $some)) {
            $this->handleCancel($some, $controller);
            return;
        }

        if (!empty($some['sub'])) {
            $this->handleSub($some, $controller);
            return;
        }

        if (!empty($some['unsub'])) {
            $controller->unsubscribe((string) $some['unsub']);
            return;
        }

        if (!empty($some['topic'])) {
            $this->handleTopic($some, $controller);
            return;
        }

        if (array_key_exists('id', $some)) {
            $this->handleResponse($some);
        }
    }

    private function handlePath(array $some, Controller $controller): void
    {
        $path = (string) $some['path'];
        $id = $some['id'] ?? null;
        $api = $this->postApi[$path] ?? null;
        if ($api === null) {
            foreach ($this->otherPostApi as $candidate) {
                if (($candidate['path'])($path)) {
                    $api = $candidate['request'];
                    break;
                }
            }
        }

        $task = null;
        try {
            if ($api === null) {
                throw new ChannelException('api not found');
            }

            if ($id !== null && (($some['cancellable'] ?? true) !== false)) {
                $task = new CancellableRequest(fn() => $controller->response(['id' => $id, 'error' => 'cancelled']));
            }

            $result = $api([
                'body' => $some['body'] ?? null,
                'context' => $some['context'] ?? [],
                'sender' => $controller->sender,
                'state' => $controller->state,
                'path' => $path,
                'task' => $task,
            ]);

            if ($id === null) {
                return;
            }

            if ($result instanceof AsyncResult) {
                if ($result->isResolved()) {
                    if ($task === null || !$task->isCancelled) {
                        $controller->response(['id' => $id, 'body' => $result->resolvedValue()]);
                    }
                    return;
                }

                if ($result->isRejected()) {
                    if ($task === null || !$task->isCancelled) {
                        $controller->response(['id' => $id, 'error' => (string) $result->rejectedError()]);
                    }
                    return;
                }

                if ($task !== null) {
                    $controller->sender->requests[(int) $id] = $task;
                }

                $result
                    ->then(function (mixed $value) use ($id, $task, $controller): void {
                        if ($task !== null && $task->isCancelled) {
                            return;
                        }
                        $controller->response(['id' => $id, 'body' => $value]);
                        if ($task !== null) {
                            unset($controller->sender->requests[(int) $id]);
                        }
                    })
                    ->catch(function (mixed $error) use ($id, $task, $controller): void {
                        if ($task !== null && $task->isCancelled) {
                            return;
                        }
                        $controller->response(['id' => $id, 'error' => (string) $error]);
                        if ($task !== null) {
                            unset($controller->sender->requests[(int) $id]);
                        }
                    });
                return;
            }

            $controller->response(['id' => $id, 'body' => $result]);
            if ($task !== null) {
                unset($controller->sender->requests[(int) $id]);
            }
        } catch (\Throwable $error) {
            if ($id !== null) {
                if ($task === null || !$task->isCancelled) {
                    $controller->response(['id' => $id, 'error' => (string) $error->getMessage()]);
                }
                if ($task !== null) {
                    unset($controller->sender->requests[(int) $id]);
                }
            }
        }
    }

    private function handleStream(array $some, Controller $controller): void
    {
        $id = $some['id'] ?? null;
        $streamPath = (string) $some['stream'];

        try {
            $api = $this->streamApi[$streamPath] ?? null;
            if ($api === null) {
                foreach ($this->otherStreamApi as $candidate) {
                    if (($candidate['path'])($streamPath)) {
                        $api = $candidate['request'];
                        break;
                    }
                }
            }

            if ($api === null) {
                throw new ChannelException('api not found');
            }
            if ($id === null) {
                throw new ChannelException('stream requires id');
            }

            $runner = function () use ($id, $controller, $streamPath, $some, $api): void {
                $this->streamRequest(
                    (int) $id,
                    $controller,
                    $streamPath,
                    $some['body'] ?? null,
                    $some['context'] ?? [],
                    $api,
                );
            };

            if (Coroutine::getCid() >= 0 && function_exists('go')) {
                go($runner);
            } else {
                $runner();
            }
        } catch (\Throwable $error) {
            if ($id !== null) {
                $controller->response(['id' => $id, 'error' => $error->getMessage()]);
            }
        }
    }

    private function handleCancel(array $some, Controller $controller): void
    {
        try {
            $id = $some['cancel'] ?? null;
            if ($id === null) {
                throw new ChannelException('cancellation requires id');
            }

            $id = (int) $id;
            $stream = $controller->sender->streams[$id] ?? null;
            if (is_object($stream) && method_exists($stream, 'return')) {
                $stream->return();
            }
            unset($controller->sender->streams[$id]);

            $request = $controller->sender->requests[$id] ?? null;
            if ($request instanceof CancellableRequest) {
                $request->cancel();
                unset($controller->sender->requests[$id]);
            }
        } catch (\Throwable) {
        }
    }

    private function handleSub(array $some, Controller $controller): void
    {
        $id = $some['id'] ?? null;
        $subscription = $this->eventsApi[(string) $some['sub']] ?? null;

        try {
            if (!$subscription instanceof Subscription) {
                throw new ChannelException('subscription not found');
            }

            $topic = $subscription->formatTopic($some['body'] ?? null);
            $topic = $subscription->prefix . '/' . $topic;
            $body = $subscription->formatBody($some['body'] ?? null);

            if ($body instanceof AsyncResult) {
                if ($body->isResolved()) {
                    $controller->subscribe($topic);
                    $response = ['topic' => $topic, 'body' => $body->resolvedValue()];
                    if ($id !== null) {
                        $response['id'] = $id;
                    }
                    $controller->response($response);
                    return;
                }

                if ($body->isRejected()) {
                    if ($id !== null) {
                        $controller->response(['id' => $id, 'error' => (string) $body->rejectedError()]);
                    }
                    return;
                }

                $body
                    ->then(function (mixed $value) use ($topic, $id, $controller): void {
                        $controller->subscribe($topic);
                        $response = ['topic' => $topic, 'body' => $value];
                        if ($id !== null) {
                            $response['id'] = $id;
                        }
                        $controller->response($response);
                    })
                    ->catch(function (mixed $error) use ($id, $controller): void {
                        if ($id !== null) {
                            $controller->response(['id' => $id, 'error' => (string) $error]);
                        }
                    });
                return;
            }

            $controller->subscribe($topic);
            $response = ['topic' => $topic, 'body' => $body];
            if ($id !== null) {
                $response['id'] = $id;
            }
            $controller->response($response);
        } catch (\Throwable $error) {
            if ($id !== null) {
                $controller->response(['id' => $id, 'error' => $error->getMessage()]);
            }
        }
    }

    private function handleTopic(array $some, Controller $controller): void
    {
        $id = $some['id'] ?? null;
        if ($id !== null) {
            $request = $this->requests[(int) $id] ?? null;
            unset($this->requests[(int) $id]);
            if ($request instanceof PendingRequest) {
                ($request->response)($some);
            }
        }

        if (!empty($some['body'])) {
            $controller->event((string) $some['topic'], $some['body']);
        }
    }

    private function handleResponse(array $some): void
    {
        $id = (int) $some['id'];
        $request = $this->requests[$id] ?? null;
        if (!$request instanceof PendingRequest) {
            return;
        }

        if (!(array_key_exists('stream', $request->request) && empty($some['done']))) {
            unset($this->requests[$id]);
        }

        ($request->response)($some);
    }

    private function streamRequest(
        int $id,
        Controller $controller,
        string $path,
        mixed $body,
        mixed $context,
        callable $stream,
    ): void {
        try {
            $result = $stream([
                'body' => $body,
                'sender' => $controller->sender,
                'state' => $controller->state,
                'path' => $path,
                'context' => $context,
            ]);

            if (is_array($result)) {
                $result = (function (array $values): \Generator {
                    foreach ($values as $value) {
                        yield $value;
                    }
                })($result);
            }

            if (!$result instanceof Traversable) {
                throw new ChannelException('stream must return iterable');
            }

            $running = new RunningStream($result);
            $controller->sender->streams[$id] = $running;

            foreach ($result as $value) {
                if ($running->cancelled) {
                    break;
                }
                $controller->response(['id' => $id, 'body' => $value]);
            }

            if (!$running->cancelled) {
                $controller->response(['id' => $id, 'done' => true]);
            }
        } catch (\Throwable $error) {
            $controller->response(['id' => $id, 'error' => $error->getMessage()]);
        }

        unset($controller->sender->streams[$id]);
    }

    public function events(array $events, string $prefix = ''): self
    {
        if ($this->eventsApi === null) {
            $this->eventsApi = [];
        }

        Subscription::parse($events, $prefix, $this->eventsApi);
        return $this;
    }

    public function api(array $api): self
    {
        $this->parseApi($api, '');
        return $this;
    }

    public function merge(Channel $channel, string $prefix = ''): self
    {
        $p = $prefix !== '' ? $prefix . '/' : '';

        if ($channel->eventsApi !== null) {
            if ($this->eventsApi === null) {
                $this->eventsApi = [];
            }
            foreach ($channel->eventsApi as $key => $subscription) {
                $this->eventsApi[$p . $key] = $subscription;
            }
        }

        foreach ($channel->postApi as $key => $handler) {
            $this->post($p . $key, $handler);
        }

        foreach ($channel->streamApi as $key => $handler) {
            $this->stream($p . $key, $handler);
        }

        $scoped = static fn (callable $match): callable => $p === ''
            ? $match
            : static fn (string $path): bool => str_starts_with($path, $p) && $match(substr($path, strlen($p)));

        foreach ($channel->otherPostApi as $route) {
            $this->postOther($scoped($route['path']), $route['request']);
        }

        foreach ($channel->otherStreamApi as $route) {
            $this->streamOther($scoped($route['path']), $route['request']);
        }

        return $this;
    }

    private function parseApi(array $api, string $prefix): void
    {
        foreach ($api as $key => $value) {
            if (!is_string($key)) {
                continue;
            }

            if (is_callable($value)) {
                $path = $key === '_' ? $prefix : ($prefix !== '' ? $prefix . '/' . $key : $key);

                $isGenerator = false;
                try {
                    $reflection = null;
                    if ($value instanceof \Closure || is_string($value)) {
                        $reflection = new \ReflectionFunction($value);
                    } elseif (is_array($value) && count($value) === 2) {
                        $reflection = new \ReflectionMethod($value[0], (string) $value[1]);
                    } elseif (is_object($value) && method_exists($value, '__invoke')) {
                        $reflection = new \ReflectionMethod($value, '__invoke');
                    }

                    if ($reflection instanceof \ReflectionFunctionAbstract) {
                        $isGenerator = $reflection->isGenerator();
                    }
                } catch (\Throwable) {
                }

                if ($isGenerator) {
                    $this->stream($path, $value);
                } else {
                    $this->post($path, $value);
                }
                continue;
            }

            if (is_array($value)) {
                $childPrefix = $prefix !== '' ? $prefix . '/' . $key : $key;
                $this->parseApi($value, $childPrefix);
            }
        }
    }

    public function connect(int|string $address, array $options = []): ClientSender
    {
        return ClientTransport::connect($this, $address, $options);
    }

    public function listen(int|string $address, array $options = []): \ChannelPhp\Transports\RunningServer
    {
        return ServerTransport::listen($this, $address, $options);
    }

    public function process(): Sender
    {
        return ProcessTransport::process($this);
    }
}
