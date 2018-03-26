<?php

namespace Uecode\Bundle\QPushBundle\Provider;

use Aws\Sns\SnsClient;
use Aws\Sqs\SqsClient;
use Doctrine\Common\Cache\Cache;
use Monolog\Logger;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Uecode\Bundle\QPushBundle\Event\Events;
use Uecode\Bundle\QPushBundle\Event\MessageEvent;
use Uecode\Bundle\QPushBundle\Event\NotificationEvent;
use Uecode\Bundle\QPushBundle\Message\Message;
use Uecode\Bundle\QPushBundle\Provider\AbstractProvider;
use Symfony\Component\Process\Exception\RuntimeException;
use Symfony\Component\Console\Exception\LogicException;

/**
 * Lots of this code has been copied from Uecode\Bundle\QPushBundle\Provider\AwsProvider but I
 * have removed any code that would create or any SNS topics, any SQS queues, or would
 * subscribe a queue to a topic. Instead this class assumes everything is pre-configured in AWS.
 * (I have a need for this in order to work with restrictive IAM roles.)
 *
 * - In addition topic_arn, and queue_arn are to be provided through $options.
 * - Support for FIFO queues are commented out.
 * - Drops support for Aws SDK v2
 *
 * @todo I'm not altogether sure about the design of this service. It does too much stuff and
 * should I think be broken up into several services. 
 *
 * @author Mark Cremer <mark@firstcoders.co.uk>
 */
class PreconfiguredAwsProvider extends AbstractProvider
{
    /** @var SqsClient */
    private $sqs;

    /** @var SnsClient */
    private $sns;

    /**
     * @param string $name
     * @param array  $options
     * @param mixed  $client
     * @param Cache  $cache
     * @param Logger $logger
     */
    public function __construct($name, array $options, $client, Cache $cache, Logger $logger)
    {
        $this->name = $name;
        $this->options = $options;
        $this->cache = $cache;
        $this->logger = $logger;
        $this->sqs = $client->createSqs();
        $this->sns = $client->createSns();
    }

    /**
     * @return string
     */
    public function getProvider()
    {
        return 'AWSP';
    }

    public function create()
    {
        return true;
    }

    /**
     * @return bool
     */
    public function destroy()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     *
     * This method will either use a SNS Topic to publish a queued message or
     * straight to SQS depending on the application configuration.
     *
     * @return string
     */
    public function publish(array $message, array $options = [])
    {
        if (array_key_exists('topic_arn', $this->options)) {
            return $this->publishToTopic($message);
        } elseif (array_key_exists('queue_url', $this->options)) {
            return $this->publishToQueue($message, $options);
        }

        throw new LogicException('Both "topic_arn" and "queue_url" are left undefined. This provider must be configured to publish either to an SNS topic or a SQS queue.');
    }

    /**
     * @param array $message
     *
     * @return string
     */
    public function publishToTopic(array $message)
    {
        $publishStart = microtime(true);

        $message = [
            'default' => $this->getNameWithPrefix(),
            'sqs' => json_encode($message),
            'http' => $this->getNameWithPrefix(),
            'https' => $this->getNameWithPrefix(),
        ];

        $result = $this->sns->publish(
            [
                'TopicArn' => $this->options['topic_arn'],
                'Subject' => $this->getName(),
                'Message' => json_encode($message),
                'MessageStructure' => 'json'
            ]
        );

        $context = [
            'TopicArn' => $this->options['topic_arn'],
            'MessageId' => $result->get('MessageId'),
            'publish_time' => microtime(true) - $publishStart,
        ];

        $this->log(200, 'Message published to SNS', $context);

        return $result->get('MessageId');
    }

    /**
     * @param array $message
     * @param array $options
     *
     * @return string
     */
    public function publishToQueue(array $message, array $options)
    {
        $mergedOptions = $this->mergeOptions($options);

        // if (isset($options['message_deduplication_id'])) {
        //     $mergedOptions['message_deduplication_id'] = $options['message_deduplication_id'];
        // }

        // if (isset($options['message_group_id'])) {
        //     $mergedOptions['message_group_id'] = $options['message_group_id'];
        // }

        $options = $mergedOptions;

        $publishStart = microtime(true);

        $arguments = [
            'QueueUrl' => $this->options['queue_url'],
            'MessageBody' => json_encode($message),
            'DelaySeconds' => $options['message_delay'],
        ];

        // if ($this->isQueueFIFO()) {
        //     if (isset($options['message_deduplication_id'])) {
        //         // Always use user supplied dedup id
        //         $arguments['MessageDeduplicationId'] = $options['message_deduplication_id'];
        //     } elseif ($options['content_based_deduplication'] !== true) {
        //         // If none is supplied and option "content_based_deduplication" is not set, generate default
        //         $arguments['MessageDeduplicationId'] = hash('sha256', json_encode($message));
        //     }

        //     $arguments['MessageGroupId'] = $this->getNameWithPrefix();
        //     if (isset($options['message_group_id'])) {
        //         $arguments['MessageGroupId'] = $options['message_group_id'];
        //     }
        // }

        $result = $this->sqs->sendMessage($arguments);

        $context = [
            'QueueUrl' => $this->options['queue_url'],
            'MessageId' => $result->get('MessageId'),
            // 'push_notifications' => $options['push_notifications'],
            'fifo' => $options['fifo'],
        ];

        // if ($this->isQueueFIFO()) {
        //     if (isset($arguments['MessageDeduplicationId'])) {
        //         $context['message_deduplication_id'] = $arguments['MessageDeduplicationId'];
        //     }
        //     $context['message_group_id'] = $arguments['MessageGroupId'];
        // }

        $this->log(200, 'Message published to SQS', $context);

        return $result->get('MessageId');
    }

    /**
     * @param array $options
     * @return void
     */
    public function receive(array $options = [])
    {
        $options = $this->mergeOptions($options);

        $result = $this->sqs->receiveMessage(
            [
                'QueueUrl' => $this->options['queue_url'],
                'MaxNumberOfMessages' => $options['messages_to_receive'],
                'WaitTimeSeconds' => $options['receive_wait_time'],
            ]
        );

        $messages = $result->get('Messages') ?: [];

        // Convert to Message Class
        foreach ($messages as &$message) {
            $id = $message['MessageId'];
            $metadata = [
                'ReceiptHandle' => $message['ReceiptHandle'],
                'MD5OfBody' => $message['MD5OfBody'],
            ];

            // When using SNS, the SQS Body is the entire SNS Message
            if (is_array($body = json_decode($message['Body'], true))
                && isset($body['Message'])
            ) {
                $body = json_decode($body['Message'], true);
            }

            $message = new Message($id, $body, $metadata);

            $context = ['MessageId' => $id];
            $this->log(200, 'Message fetched from SQS Queue', $context);
        }

        return $messages;
    }

    /**
     * {@inheritdoc}
     *
     * @return bool
     */
    public function delete($id)
    {
        $this->sqs->deleteMessage(
            [
                'QueueUrl' => $this->options['queue_url'],
                'ReceiptHandle' => $id,
            ]
        );

        $context = [
            'QueueUrl' => $this->options['queue_url'],
            'ReceiptHandle' => $id,
        ];
        $this->log(200, 'Message deleted from SQS Queue', $context);

        return true;
    }

    /**
     * Removes the message from queue after all other listeners have fired.
     *
     * If an earlier listener has erred or stopped propagation, this method
     * will not fire and the Queued Message should become visible in queue again.
     *
     * Stops Event Propagation after removing the Message
     *
     * @param MessageEvent $event The SQS Message Event
     */
    public function onMessageReceived(MessageEvent $event)
    {
        $receiptHandle = $event
            ->getMessage()
            ->getMetadata()
            ->get('ReceiptHandle');

        $this->delete($receiptHandle);

        $event->stopPropagation();
    }

    /**
     * Handles SNS Notifications.
     *
     * For Subscription notifications, this method will automatically confirm
     * the Subscription request
     *
     * For Message notifications, this method polls the queue and dispatches
     * the `{queue}.message_received` event for each message retrieved
     *
     * @param NotificationEvent        $event      The Notification Event
     * @param string                   $eventName  Name of the event
     * @param EventDispatcherInterface $dispatcher
     */
    public function onNotification(NotificationEvent $event, $eventName, EventDispatcherInterface $dispatcher)
    {
        throw new RuntimeException('Push notifications are disabled with this provider');

        // if (NotificationEvent::TYPE_SUBSCRIPTION == $event->getType()) {
        //     $topicArn = $event->getNotification()->getMetadata()->get('TopicArn');
        //     $token = $event->getNotification()->getMetadata()->get('Token');

        //     $this->sns->confirmSubscription([
        //         'TopicArn' => $topicArn,
        //         'Token' => $token,
        //     ]);

        //     $context = ['TopicArn' => $topicArn];
        //     $this->log(200, 'Subscription to SNS Confirmed', $context);

        //     return;
        // }

        // $messages = $this->receive();
        // foreach ($messages as $message) {
        //     $messageEvent = new MessageEvent($this->name, $message);
        //     $dispatcher->dispatch(Events::Message($this->name), $messageEvent);
        // }
    }

    /*
     * @return bool
     */
    // private function isQueueFIFO()
    // {
    //     return $this->options['fifo'] === true;
    // }
}
