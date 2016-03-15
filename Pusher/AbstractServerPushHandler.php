<?php

namespace Gos\Bundle\WebSocketBundle\Pusher;

use Gos\Bundle\WebSocketBundle\Pusher\Serializer\MessageSerializer;
use Gos\Bundle\WebSocketBundle\Router\WampRouter;
use Gos\Bundle\WebSocketBundle\Server\App\WampApplication;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Ratchet\Wamp\Topic;
use Ratchet\Wamp\WampServerInterface;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

abstract class AbstractServerPushHandler implements ServerPushHandlerInterface
{
    /** @var  string */
    private $name;

    /** @var  string */
    private $config;

    /** @var PusherInterface  */
    protected $pusher;

    /** @var  LoggerInterface */
    protected $logger;

    /** @var  WampRouter */
    protected $router;

    /** @var  MessageSerializer */
    protected $serializer;

    /** @var  EventDispatcherInterface */
    protected $eventDispatcher;

    /**
     * @param PusherInterface          $pusher
     * @param WampRouter               $router
     * @param MessageSerializer        $serializer
     * @param EventDispatcherInterface $eventDispatcher
     * @param LoggerInterface|null     $logger
     */
    public function __construct(
        PusherInterface $pusher,
        WampRouter $router,
        MessageSerializer $serializer,
        EventDispatcherInterface $eventDispatcher,
        LoggerInterface $logger = null
    ) {
        $this->pusher = $pusher;
        $this->router = $router;
        $this->serializer = $serializer;
        $this->eventDispatcher = $eventDispatcher;
        $this->logger = $logger === null ? new NullLogger() : $logger;
    }

    /**
     * @param string $name
     */
    public function setName($name)
    {
        $this->name = $name;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param array $config
     */
    public function setConfig(array $config)
    {
        $this->config = $config;
    }

    /**
     * @param WampServerInterface|WampApplication $app
     * @param string                              $data
     */
    protected function doPush(WampServerInterface $app, $data)
    {
        /** @var MessageInterface $message */
        $message = $this->serializer->deserialize($data);
        $topic = new Topic($message->getTopic());
        $request = $this->router->match($topic);

        $app->onPush($request, $message->getData(), $this->getName());
    }
}
