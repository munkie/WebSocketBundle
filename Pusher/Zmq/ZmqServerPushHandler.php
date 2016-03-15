<?php

namespace Gos\Bundle\WebSocketBundle\Pusher\Zmq;

use Gos\Bundle\WebSocketBundle\Event\Events;
use Gos\Bundle\WebSocketBundle\Event\PushHandlerEvent;
use Gos\Bundle\WebSocketBundle\Pusher\AbstractServerPushHandler;
use Gos\Bundle\WebSocketBundle\Pusher\MessageInterface;
use Ratchet\Wamp\Topic;
use Ratchet\Wamp\WampServerInterface;
use React\EventLoop\LoopInterface;
use React\ZMQ\Context;
use React\ZMQ\SocketWrapper;

class ZmqServerPushHandler extends AbstractServerPushHandler
{
    /** @var  Context */
    protected $consumer;

    /**
     * @param LoopInterface       $loop
     * @param WampServerInterface $app
     */
    public function handle(LoopInterface $loop, WampServerInterface $app)
    {
        $config = $this->getConfig();

        $context = new Context($loop);

        /* @var SocketWrapper $pull */
        $this->consumer = $context->getSocket(\ZMQ::SOCKET_PULL);

        $this->logger->info(sprintf(
            'ZMQ transport listening on %s:%s',
            $config['host'],
            $config['port']
        ));

        $this->consumer->bind('tcp://' . $config['host'] . ':' . $config['port']);

        $this->consumer->on('message', function ($data) use ($app, $config) {

            try {
                $this->doPush($app, $data);

                $this->eventDispatcher->dispatch(Events::PUSHER_SUCCESS, new PushHandlerEvent($data, $this));
            } catch (\Exception $e) {
                $this->logger->error(
                    'AMQP handler failed to ack message', [
                        'exception_message' => $e->getMessage(),
                        'file' => $e->getFile(),
                        'line' => $e->getLine(),
                        'message' => $data,
                    ]
                );

                $this->eventDispatcher->dispatch(Events::PUSHER_FAIL, new PushHandlerEvent($data, $this));
            }

        });
    }

    public function close()
    {
        $this->consumer->close();
    }
}
