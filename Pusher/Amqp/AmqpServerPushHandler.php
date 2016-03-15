<?php

namespace Gos\Bundle\WebSocketBundle\Pusher\Amqp;

use Gos\Bundle\WebSocketBundle\Event\Events;
use Gos\Bundle\WebSocketBundle\Event\PushHandlerEvent;
use Gos\Bundle\WebSocketBundle\Pusher\AbstractServerPushHandler;
use Gos\Bundle\WebSocketBundle\Pusher\MessageInterface;
use Gos\Component\ReactAMQP\Consumer;
use Ratchet\Wamp\Topic;
use Ratchet\Wamp\WampServerInterface;
use React\EventLoop\LoopInterface;

class AmqpServerPushHandler extends AbstractServerPushHandler
{
    /** @var  Consumer */
    protected $consumer;

    /**
     * @param LoopInterface       $loop
     * @param WampServerInterface $app
     */
    public function handle(LoopInterface $loop, WampServerInterface $app)
    {
        $config = $this->pusher->getConfig();

        $connection = new \AMQPConnection($config);
        $connection->connect();

        list(, , $queue) = Utils::setupConnection($connection, $config);

        $this->consumer = new Consumer($queue, $loop, 0.1, 10);
        $this->consumer->on('consume', function (\AMQPEnvelope $envelop, \AMQPQueue $queue) use ($app, $config) {

            try {
                $this->doPush($app, $envelop->getBody());
                
                $queue->ack($envelop->getDeliveryTag());
                $this->eventDispatcher->dispatch(Events::PUSHER_SUCCESS, new PushHandlerEvent($envelop->getBody(), $this));
            } catch (\Exception $e) {
                $this->logger->error(
                    'AMQP handler failed to ack message', [
                        'exception_message' => $e->getMessage(),
                        'file' => $e->getFile(),
                        'line' => $e->getLine(),
                        'message' => $envelop->getBody(),
                    ]
                );

                $queue->reject($envelop->getDeliveryTag());
                $this->eventDispatcher->dispatch(Events::PUSHER_FAIL, new PushHandlerEvent($envelop->getBody(), $this));
            }

            $this->logger->info(sprintf(
                'AMQP transport listening on %s:%s',
                $config['host'],
                $config['port']
            ));
        });
    }

    public function close()
    {
        if (null !== $this->consumer) {
            $this->consumer->emit('close_amqp_consumer');
        }
    }
}
