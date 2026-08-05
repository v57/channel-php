<?php
declare(strict_types=1);

namespace ChannelPhp\Transports;

use ChannelPhp\Channel;
use ChannelPhp\ClientSender;
use ChannelPhp\Controller;
use ChannelPhp\Subscription;
use ChannelPhp\SubscriptionEvent;
use function ChannelPhp\makeSender;

final class ClientTransport
{
    public static function connect(Channel $channel, int|string $address, array $options = []): ClientSender
    {
        $target = is_string($address) ? $address : 'ws://127.0.0.1:' . $address;
        $headers = $options['headers'] ?? null;
        $onConnect = $options['onConnect'] ?? null;

        $ws = new WebSocketClient($target, $headers);
        $topics = [];
        $sender = makeSender($channel, $ws);
        $state = new \stdClass();

        if (is_callable($onConnect)) {
            $ws->setOnOpen(static function () use ($onConnect, $sender): void {
                $onConnect($sender);
            });
        }

        $controller = new Controller(
            responseImpl: static function (array $response) use ($ws): void {
                $ws->notify($response);
            },
            subscribeImpl: static function (string $topic) use (&$topics): void {
                $topics[$topic] = true;
            },
            unsubscribeImpl: static function (string $topic) use (&$topics): void {
                unset($topics[$topic]);
            },
            eventImpl: static function (string $topic, mixed $event) use ($ws): void {
                $ws->receivedEvent($topic, $event);
            },
            sender: $sender,
            state: $state,
        );

        $publishers = [];
        $removedPublishers = false;

        $cleanupPublishers = static function () use (&$removedPublishers, &$publishers): void {
            if ($removedPublishers) {
                return;
            }

            $removedPublishers = true;
            foreach ($publishers as $item) {
                /** @var Subscription $subscription */
                $subscription = $item['subscription'];
                $publisher = $item['publisher'];
                $index = array_search($publisher, $subscription->publishers, true);
                if ($index !== false) {
                    unset($subscription->publishers[$index]);
                    $subscription->publishers = array_values($subscription->publishers);
                }
            }
            $publishers = [];
        };

        $ws->setOnClose(static function () use ($ws, $cleanupPublishers, $channel, $state, $sender): void {
            if (!$ws->isRunning()) {
                $cleanupPublishers();
            }
            $channel->disconnect($state, $sender);
        });

        $ws->setOnMessage(static function (mixed $message) use ($channel, $controller): void {
            $channel->receive($message, $controller);
        });

        if ($channel->eventsApi !== null) {
            foreach ($channel->eventsApi as $subscription) {
                $publisher = static function (SubscriptionEvent $event) use ($ws, &$topics): void {
                    if (!isset($topics[$event->topic])) {
                        return;
                    }
                    $ws->notify(['topic' => $event->topic, 'body' => $event->body]);
                };

                $subscription->publishers[] = $publisher;
                $publishers[] = ['subscription' => $subscription, 'publisher' => $publisher];
            }
        }

        return new ClientSender($sender, $ws);
    }
}
