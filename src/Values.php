<?php
declare(strict_types=1);

namespace ChannelPhp;

use ChannelPhp\Internal\Waiter;
use Generator;

final class Values implements \IteratorAggregate
{
    private bool $isRunning = false;
    private bool $isClosed = false;

    /** @var array<int, array<string, mixed>> */
    private array $queued = [];

    /** @var array<int, Waiter> */
    private array $pending = [];

    private ?int $rid = null;

    public function __construct(
        private readonly Channel $channel,
        private readonly string $path,
        private readonly mixed $body,
        private readonly mixed $context,
        private readonly mixed $onSend,
        private readonly mixed $onCancel,
        private readonly Sender $sender,
    ) {
    }

    private function start(): void
    {
        if ($this->isRunning) {
            return;
        }

        $this->isRunning = true;
        $request = $this->channel->makeStream(
            $this->path,
            $this->body,
            $this->context,
            fn(array $response) => $this->response($response),
            $this->sender,
        );

        $this->rid = $request['id'];
        ($this->onSend)($request);
    }

    private function response(array $response): void
    {
        $waiter = array_shift($this->pending);
        if ($waiter instanceof Waiter) {
            $waiter->resolve($response);
            return;
        }

        $this->queued[] = $response;
    }

    private function nextResponse(): array
    {
        if ($this->queued !== []) {
            return array_shift($this->queued);
        }

        $waiter = new Waiter();
        $this->pending[] = $waiter;
        $this->start();

        /** @var array<string, mixed> */
        return $waiter->await();
    }

    private function processResponse(array $response): array
    {
        if (array_key_exists('error', $response) && $response['error'] !== null) {
            $this->rid = null;
            throw ChannelException::from($response['error']);
        }

        if (!empty($response['done'])) {
            $this->rid = null;
            $this->isClosed = true;
            return ['value' => null, 'done' => true];
        }

        return ['value' => $response['body'] ?? null, 'done' => false];
    }

    public function next(): array
    {
        if ($this->isClosed) {
            return ['value' => null, 'done' => true];
        }

        return $this->processResponse($this->nextResponse());
    }

    public function cancel(): void
    {
        if ($this->rid === null) {
            return;
        }

        $rid = $this->rid;
        $this->rid = null;
        $this->isClosed = true;
        ($this->onCancel)($rid);

        foreach ($this->pending as $waiter) {
            if ($waiter instanceof Waiter) {
                $waiter->resolve(['done' => true]);
            }
        }
        $this->pending = [];
        $this->queued = [];
    }

    public function returnStream(): array
    {
        $this->cancel();
        $this->isClosed = true;
        return ['value' => null, 'done' => true];
    }

    public function getIterator(): \Traversable
    {
        return $this->iterate();
    }

    private function iterate(): Generator
    {
        try {
            while (true) {
                $value = $this->next();
                if ($value['done']) {
                    break;
                }

                yield $value['value'];
            }
        } finally {
            $this->cancel();
        }
    }
}
