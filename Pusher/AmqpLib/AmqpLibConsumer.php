<?php

namespace Gos\Bundle\WebSocketBundle\Pusher\AmqpLib;

use Evenement\EventEmitter;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use React\EventLoop\LoopInterface;

class AmqpLibConsumer extends EventEmitter
{
    /**
     * @var AMQPChannel
     */
    private $channel;

    /**
     * @var LoopInterface
     */
    private $loop;

    public function __construct(AMQPChannel $channel, LoopInterface $loop)
    {
        $this->channel = $channel;
        $this->loop = $loop;

        $this->loop->addReadStream(
            $channel->getConnection()->getSocket()->getSocket(),
            function () {
                $this->channel->wait(null, true);
            }
        );
    }

    /**
     * @param string $queueName
     */
    public function consume($queueName)
    {
        $this->channel->basic_consume(
            $queueName,
            null,
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) {
                $this->emit('message', [$message]);
            }
        );
    }
}
