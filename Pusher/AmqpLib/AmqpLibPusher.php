<?php

namespace Gos\Bundle\WebSocketBundle\Pusher\AmqpLib;

use Gos\Bundle\WebSocketBundle\Pusher\AbstractPusher;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Symfony\Component\OptionsResolver\OptionsResolver;

class AmqpLibPusher extends AbstractPusher
{
    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @var string
     */
    protected $exchangeName;

    /**
     * AmqpLibPusher constructor.
     *
     * @param AMQPChannel $channel
     * @param string      $exchangeName
     */
    public function __construct(AMQPChannel $channel, $exchangeName)
    {
        $this->channel = $channel;
        $this->exchangeName = $exchangeName;
    }

    /**
     * {@inheritdoc}
     */
    protected function doPush($data, array $context)
    {
        $context = $this->resolveContext($context);

        $message = new AMQPMessage($data, $context['attributes']);
        $this->channel->basic_publish(
            $message,
            $this->exchangeName,
            $context['routing_key']
        );
    }

    /**
     * @param array $context
     *
     * @return array
     */
    protected function resolveContext(array $context)
    {
        $resolver = new OptionsResolver();

        $resolver->setDefaults([
            'routing_key' => '',
            'attributes' => [],
        ]);

        return $resolver->resolve($context);
    }

    public function close()
    {
        $this->channel->close();
        $this->channel->getConnection()->close();
    }
}
