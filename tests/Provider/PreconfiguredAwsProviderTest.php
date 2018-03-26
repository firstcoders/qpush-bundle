<?php

namespace Uecode\Bundle\QPushBundle\Tests\Provider;

use Uecode\Bundle\QPushBundle\Provider\PreconfiguredAwsProvider;
use PHPUnit\Framework\TestCase;
use Monolog\Logger;
use Doctrine\Common\Cache\Cache;
use Aws\Sdk;
use Aws\Sqs\SqsClient;
use Aws\Sns\SnsClient;
use Aws\ResultInterface;
use Uecode\Bundle\QPushBundle\Tests\Event\MessageEventTest;
use Uecode\Bundle\QPushBundle\Message\Message;

class PreconfiguredAwsProviderTest extends \PHPUnit_Framework_TestCase
{
    private $mockCache;
    private $mockLogger;
    private $mockClient;
    private $mockSns;
    private $mockSqs;

    public function setUp()
    {
        $this->mockCache = $this->getMock(Cache::class);
        $this->mockLogger = $this->getMock(Logger::class, [], ['qpush.test']);
        
        $this->mockClient = $this->getMockBuilder(Sdk::class)
            ->disableOriginalConstructor()
            ->setMethods(['createSqs', 'createSns'])
            ->getMock();

        $this->mockSqs = $this->getMockBuilder(SqsClientSdk::class)
            ->disableOriginalConstructor()
            ->setMethods(['sendMessage', 'receiveMessage', 'deleteMessage'])
            ->getMock();

        $this->mockSns = $this->getMockBuilder(SnsClient::class)
            ->disableOriginalConstructor()
            ->setMethods(['publish'])
            ->getMock();

        

        // $this->mockSns = $this->getMock(SnsClient::class, ['__call', 'publish']);
        
        $this->mockClient
            ->expects($this->any())
            ->method('createSqs')
            ->will($this->returnValue($this->mockSqs));
        
        $this->mockClient
            ->expects($this->any())
            ->method('createSns')
            ->will($this->returnValue($this->mockSns));

        // Make sure certain disallowed methods are never called
        $snsMethods = [
            'createTopic',
            'listSubscriptionsByTopic',
            'subscribe',
            'confirmSubscription',
            'unsubscribe',
            'deleteTopic',
            'getTopicAttributes'
        ];
        
        foreach ($snsMethods as $method) {
            $this->setNoOpExpectation($this->mockSns, $method);
        }

        $sqsMethods = [
            'getQueueUrl',
            'createQueue',
            'setQueueAttributes',
            'getQueueArn',
            'deleteQueue',
        ];
        
        foreach ($sqsMethods as $method) {
            $this->setNoOpExpectation($this->mockSqs, $method);
        }
    }

    private function createProvider(array $options = [])
    {
        $options = array_merge([
            'logging_enabled' => true,
            'messages_to_receive' => 1,
            'receive_wait_time' => 10
        ], $options);

        $provider = new PreconfiguredAwsProvider(
            'testProvider',
            $options,
            $this->mockClient,
            $this->mockCache,
            $this->mockLogger
        );

        return $provider;
    }

    public function testCreateDoesNothingButReturnsTrue()
    {
        $provider = $this->createProvider();
        $this->assertTrue($provider->create());
    }

    public function testDestroyDoesNothingButReturnsTrue()
    {
        $provider = $this->createProvider();
        $this->assertTrue($provider->destroy());
    }

    public function testPublishPublishesToSnsTopic()
    {
        $mockResult = $this->getMock(ResultInterface::class);
         
        $this->mockSns
            ->expects($this->once())
            ->method('publish')
            ->with($this->callback(function ($v) {
                return $v['TopicArn'] === 'abcde/my_topic';
            }))
            ->will($this->returnValue($mockResult));

        $this->setNoOpExpectation($this->mockSqs, 'sendMessage');

        $message = ['a' => 'b'];
        $provider = $this->createProvider(['topic_arn' => 'abcde/my_topic']);
        $provider->publish([$message]);
    }

    public function testPublishPublishesToSqsQueue()
    {
        $mockResult = $this->getMock(ResultInterface::class);
         
        $this->mockSqs
            ->expects($this->once())
            ->method('sendMessage')
            ->with($this->callback(function ($v) {
                return $v['QueueUrl'] === 'abcde/my_queue';
            }))
            ->will($this->returnValue($mockResult));

        $this->setNoOpExpectation($this->mockSns, 'publish');

        $message = ['a' => 'b'];
        $provider = $this->createProvider(['queue_url' => 'abcde/my_queue', 'message_delay' => 0, 'fifo' => false, ]);
        $provider->publish([$message]);
    }

    public function testReceiveMessagesFromSqsQueue()
    {
        $mockResult = $this->getMock(ResultInterface::class);

        $messages = [
            ['MessageId' => '123', 'ReceiptHandle' => 'abc', 'MD5OfBody' => 'aaa', 'Body' => 'bbb']
        ];

        $mockResult
            ->expects($this->any())
            ->method('get')
            ->will($this->returnValue($messages));

        $this->mockSqs
            ->expects($this->once())
            ->method('receiveMessage')
            ->with($this->callback(function ($v) {
                return $v['QueueUrl'] === 'abcde/my_queue';
            }))
            ->will($this->returnValue($mockResult));

        $provider = $this->createProvider([
            'queue_url' => 'abcde/my_queue'
        ]);

        $messages = $provider->receive();

        $this->assertCount(1, $messages);
        $this->assertInstanceOf(Message::class, $messages[0]);
    }

    public function testDelete2()
    {
        $this->mockSqs
            ->expects($this->once())
            ->method('deleteMessage')
            ->with($this->callback(function ($v) {
                return $v['QueueUrl'] === 'abcde/my_queue' && $v['ReceiptHandle'] === 1;
            }));

        $provider = $this->createProvider(['queue_url' => 'abcde/my_queue']);
        $this->assertTrue($provider->delete(1));
    }

    public function testOnMessageReceivedDeletesMessage()
    {
        $this->mockSqs
            ->expects($this->once())
            ->method('deleteMessage')
            ->with($this->callback(function ($v) {
                return $v['QueueUrl'] === 'abcde/my_queue' && $v['ReceiptHandle'] === 1;
            }));

        $provider = $this->createProvider(['queue_url' => 'abcde/my_queue']);
        $this->assertTrue($provider->delete(1));
    }

    protected function setNoOpExpectation($mock, $method)
    {
        $mock
            ->expects($this->never())
            ->method($method);
    }

    // public function testPublishPublishesToFifoSqsQueue()
    // {
           // @todo This functionality is disabled for now
    // }

    // public function testOnNotification()
    // {
    //     // @todo This functionality is disabled for now
    // }
}
