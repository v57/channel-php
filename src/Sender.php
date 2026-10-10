<?php
declare(strict_types=1);

namespace ChannelPhp;

use ChannelPhp\Internal\Waiter;

class Sender
{
    /** @var array<int, mixed> */
    public array $streams = [];

    /** @var array<int, CancellableRequest> */
    public array $requests = [];

    public function __construct(
        protected readonly Channel $channel,
        protected readonly ConnectionInterface $connection,
    ) {
    }

    public function send(string $path, mixed $body = null, mixed $context = null): mixed
    {
        $waiter = null;
        $resolved = false;
        $value = null;
        $error = null;
        $sendId = null;
        $sentNotified = false;

        $request = $this->channel->makeRequest(
            $path,
            $body,
            $context,
            function (array $response) use (&$sendId, &$sentNotified, &$resolved, &$value, &$error, &$waiter): void {
                if (array_key_exists('error', $response) && $response['error'] !== null) {
                    $error = $response['error'];
                    if ($waiter instanceof Waiter) {
                        $waiter->reject($error);
                    }
                } else {
                    $value = $response['body'] ?? null;
                    if ($waiter instanceof Waiter) {
                        $waiter->resolve($value);
                    }
                }

                $resolved = true;
                if ($sendId !== null) {
                    $this->connection->sent($sendId);
                    $sentNotified = true;
                }
            },
            $this,
            false,
        );

        $sendId = $this->connection->send($request);
        if ($resolved && !$sentNotified && $sendId !== null) {
            $this->connection->sent($sendId);
        }

        if (!$resolved) {
            $waiter = new Waiter();
            if ($resolved) {
                if ($error !== null) {
                    $waiter->reject($error);
                } else {
                    $waiter->resolve($value);
                }
            }

            return $waiter->await();
        }

        if ($error !== null) {
            if ($error instanceof \Throwable) {
                throw $error;
            }
            throw ChannelException::from($error);
        }

        return $value;
    }

    public function request(string $path, mixed $body = null, mixed $context = null): RequestHandle
    {
        $waiter = new Waiter();
        $sendId = null;
        $requestId = null;

        $request = $this->channel->makeRequest(
            $path,
            $body,
            $context,
            function (array $response) use (&$sendId, &$requestId, $waiter): void {
                if (array_key_exists('error', $response) && $response['error'] !== null) {
                    $waiter->reject($response['error']);
                } else {
                    $waiter->resolve($response['body'] ?? null);
                }

                if ($sendId !== null) {
                    $this->connection->sent($sendId);
                    $sendId = null;
                    $requestId = null;
                }
            },
            $this,
            true,
        );

        $requestId = $request['id'];
        $sendId = $this->connection->send($request);

        return new RequestHandle(
            $waiter,
            function () use (&$sendId, &$requestId): void {
                if ($sendId !== null) {
                    $cancelled = $this->connection->cancel($sendId);
                    if (!$cancelled && $requestId !== null) {
                        $this->connection->notify(['cancel' => $requestId]);
                    }
                } elseif ($requestId !== null) {
                    $this->connection->notify(['cancel' => $requestId]);
                }

                if ($requestId !== null) {
                    $this->channel->cancel($requestId);
                }

                $sendId = null;
                $requestId = null;
            },
        );
    }

    public function values(string $path, mixed $body = null, mixed $context = null): Values
    {
        $sendId = null;

        return new Values(
            $this->channel,
            $path,
            $body,
            $context,
            function (array $payload) use (&$sendId): void {
                $sendId = $this->connection->send($payload);
            },
            function (int $rid) use (&$sendId): void {
                if ($sendId !== null) {
                    $cancelled = $this->connection->cancel($sendId);
                    if (!$cancelled) {
                        $this->connection->notify(['cancel' => $rid]);
                    }
                } else {
                    $this->connection->notify(['cancel' => $rid]);
                }

                $this->channel->cancel($rid);
                $sendId = null;
            },
            $this,
        );
    }

    public function subscribe(
        string $path,
        mixed $body,
        callable $event,
        mixed $context = null,
    ): Cancellable {
        $waiter = new Waiter();
        $request = $this->channel->makeSubscription(
            $path,
            $body,
            $context,
            function (array $response) use ($event, $waiter): void {
                if (array_key_exists('error', $response) && $response['error'] !== null) {
                    $waiter->reject($response['error']);
                    return;
                }

                $topic = (string) ($response['topic'] ?? '');
                $cancelTopic = $this->connection->addTopic($topic, $event);
                $waiter->resolve(new SubscriptionHandle(function () use ($cancelTopic, $topic): void {
                    if ($cancelTopic()) {
                        $this->connection->notify(['unsub' => $topic]);
                    }
                }));
            },
            $this,
        );

        $this->connection->send($request);

        /** @var Cancellable */
        return $waiter->await();
    }

    public function stop(): void
    {
        $this->connection->stop();
    }
}
