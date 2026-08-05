<?php
declare(strict_types=1);

namespace ChannelPhp;

final class CancellableRequest implements Cancellable
{
    public bool $isCancelled = false;

    /** @var array<int, callable> */
    public array $cancelActions = [];

    public function __construct(private readonly mixed $cancelImpl = null)
    {
    }

    public function onCancel(callable $action): void
    {
        $this->cancelActions[] = $action;
    }

    public function cancel(): void
    {
        $this->isCancelled = true;
        foreach ($this->cancelActions as $action) {
            try {
                $action();
            } catch (\Throwable) {
            }
        }

        if ($this->cancelImpl !== null) {
            ($this->cancelImpl)();
        }
    }
}
