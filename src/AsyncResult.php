<?php
declare(strict_types=1);

namespace ChannelPhp;

final class AsyncResult
{
    private static ?self $resolvedNull = null;
    private static ?self $resolvedFalse = null;
    private static ?self $resolvedTrue = null;
    private static ?self $resolvedZero = null;
    private static ?self $resolvedOne = null;

    private bool $resolved = false;
    private bool $rejected = false;
    private mixed $value = null;
    private mixed $error = null;

    /** @var array<int, callable> */
    private array $thenCallbacks = [];

    /** @var array<int, callable> */
    private array $catchCallbacks = [];

    public static function resolved(mixed $value): self
    {
        if ($value === null) {
            return self::$resolvedNull ??= self::createResolved($value);
        }

        if ($value === false) {
            return self::$resolvedFalse ??= self::createResolved($value);
        }

        if ($value === true) {
            return self::$resolvedTrue ??= self::createResolved($value);
        }

        if ($value === 0) {
            return self::$resolvedZero ??= self::createResolved($value);
        }

        if ($value === 1) {
            return self::$resolvedOne ??= self::createResolved($value);
        }

        return self::createResolved($value);
    }

    private static function createResolved(mixed $value): self
    {
        $result = new self();
        $result->resolve($value);
        return $result;
    }

    public static function rejected(mixed $error): self
    {
        $result = new self();
        $result->reject($error);
        return $result;
    }

    public function then(callable $callback): self
    {
        if ($this->resolved) {
            $callback($this->value);
            return $this;
        }

        if (!$this->rejected) {
            $this->thenCallbacks[] = $callback;
        }

        return $this;
    }

    public function catch(callable $callback): self
    {
        if ($this->rejected) {
            $callback($this->error);
            return $this;
        }

        if (!$this->resolved) {
            $this->catchCallbacks[] = $callback;
        }

        return $this;
    }

    public function resolve(mixed $value): void
    {
        if ($this->resolved || $this->rejected) {
            return;
        }

        $this->resolved = true;
        $this->value = $value;
        foreach ($this->thenCallbacks as $callback) {
            $callback($value);
        }
        $this->thenCallbacks = [];
        $this->catchCallbacks = [];
    }

    public function reject(mixed $error): void
    {
        if ($this->resolved || $this->rejected) {
            return;
        }

        $this->rejected = true;
        $this->error = $error;
        foreach ($this->catchCallbacks as $callback) {
            $callback($error);
        }
        $this->thenCallbacks = [];
        $this->catchCallbacks = [];
    }

    public function isResolved(): bool
    {
        return $this->resolved;
    }

    public function isRejected(): bool
    {
        return $this->rejected;
    }

    public function resolvedValue(): mixed
    {
        return $this->value;
    }

    public function rejectedError(): mixed
    {
        return $this->error;
    }
}
