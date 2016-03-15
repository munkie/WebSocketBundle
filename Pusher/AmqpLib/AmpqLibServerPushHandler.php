<?php

namespace Gos\Bundle\WebSocketBundle\Pusher\AmqpLib;

use Gos\Bundle\WebSocketBundle\Event\Events;
use Gos\Bundle\WebSocketBundle\Event\PushHandlerEvent;
use Gos\Bundle\WebSocketBundle\Pusher\AbstractServerPushHandler;
use Gos\Bundle\WebSocketBundle\Server\App\WampApplication;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Ratchet\Wamp\WampServerInterface;
use React\EventLoop\LoopInterface;

class AmpqLibServerPushHandler extends AbstractServerPushHandler
{
    /**
     * @var AmqpLibConsumer
     */
    protected $consumer;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @param LoopInterface                       $loop
     * @param WampServerInterface|WampApplication $app
     */
    public function handle(LoopInterface $loop, WampServerInterface $app)
    {
        $this->consumer = new AmqpLibConsumer($this->channel, $loop);

        $this->consumer->on('message', function (AMQPMessage $msg) use ($app) {
            try {
                $this->doPush($app, $msg->getBody());

                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                $this->eventDispatcher->dispatch(Events::PUSHER_SUCCESS, new PushHandlerEvent($msg->getBody(), $this));
            } catch (\Exception $e) {
                $this->logger->error(
                    'AmqpLib handler failed to ack message',
                    [
                        'exception_message' => $e->getMessage(),
                        'file' => $e->getFile(),
                        'line' => $e->getLine(),
                        'message' => $msg->getBody(),
                    ]
                );
                $msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], false);
                $this->eventDispatcher->dispatch(Events::PUSHER_FAIL, new PushHandlerEvent($msg->getBody(), $this));
            }
        });
    }

    public function close()
    {
        $this->channel->close();
        $this->channel->getConnection()->close();
    }
}
