<?php
App::uses('QueueDaemonShell', 'QueueDaemon.Console');
require_once CakePlugin::path('QueueDaemon') . 'Vendor/autoload.php';

class AWSSqsShell extends QueueDaemonShell
{

    public $AwsSqsClient = null;

    public $configApp = 'default';

    public $monitQueueDelay = 500000; // .5 seconds
    
    public $queue_priorities = array(
        'high',
        'normal',
        'low'
    );

    private $_queue_urls = array();

    private $_receipts_handlers = array();

    protected $jobs = array();

    public function initialize()
    {
        parent::initialize();
        if (! Configure::read('QueueDaemon.AWS')) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.AWS] ');
            die();
        }
        
        if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues')) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.queues] ');
            die();
        }
        
        if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.uuid')) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.uuid] ');
            die();
        }
        
        $this->AwsSqsClient = \Aws\Sqs\SqsClient::factory(array(
            'region' => Configure::read('QueueDaemon.AWS.region'),
            'version' => Configure::read('QueueDaemon.AWS.version'),
            'credentials' => array(
                'key' => Configure::read('QueueDaemon.AWS.key_id'),
                'secret' => Configure::read('QueueDaemon.AWS.key_secret')
            )
        ));
        
        foreach ($this->queue_priorities as $prio) {
            if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio)) {
                CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio . ']');
                die();
            }
            $this->_queue_urls[$prio] = $this->getQueueUrl(Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio));
        }
    }

    /**
     */
    public function monitQueue()
    {
        foreach ($this->queue_priorities as $priority)
            $this->jobs[$priority] = array();
        while (true) {
            foreach ($this->queue_priorities as $priority) {
                $job = $this->getQueuedCommands($priority);
                if (! empty($job))
                    $this->jobs[$priority][] = $job;
            }
            
            reset($this->queue_priorities);
            $jobDispatched = false;
            foreach ($this->queue_priorities as $priority) {
                if (count($this->jobs[$priority]) > 0) {
                    foreach ($this->jobs[$priority] as $idx => $command_data) {
                        $this->processJob($command_data['messageId'], $command_data['command'], $command_data['params'], $priority);
                        $jobDispatched = true;
                        unset($this->jobs[$priority][$idx]);
                    }
                    // if we found
                    break;
                }
            }
            if ($jobDispatched)
                continue;
            usleep($this->monitQueueDelay);
        }
    }

    /**
     *
     * @param string $command
     * @param array $params
     * @param string $prio
     * @return \Aws\Result|boolean
     */
    public function enqueueCommand(string $command, array $params, $prio = 'normal', $dedupProtect = false)
    {
        if (! in_array($prio, $this->queue_priorities)) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Data [Priority:' . $prio . ']');
            return false;
        }
        
        if (empty($command) || empty($params)) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Data [command|content]');
            return false;
        }
        
        $queue_url = $this->_queue_urls[$prio];
        
        $messageAttributes = array(
            "command" => array(
                'DataType' => "String",
                'StringValue' => $command
            )
        );
        
        $messageBody = serialize($params);
        $sendResult = $this->sendMessage($queue_url, $messageAttributes, $messageBody, $dedupProtect)->toArray();
        if ($sendResult['@metadata']['statusCode'] == 200) {
            // CakeLog::debug(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Sendind Command ' . $command . ' [' . $sendResult['MessageId'] . ']');
            return $sendResult['MessageId'];
        }
        return false;
    }

    /**
     *
     * @param string $prio
     * @param number $max_messages
     * @return \Aws\Result|NULL|boolean
     */
    public function getQueuedCommands($prio = 'normal', $max_messages = 1)
    {
        if (! in_array($prio, $this->queue_priorities)) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Data [Priority:' . $prio . ']');
            return false;
        }
        
        $messages = $this->readMessages($this->_queue_urls[$prio], $max_messages);
        
        if (is_array($messages)) {
            $commands = array();
            foreach ($messages as $msg) {
                if (empty($msg['MessageAttributes']['command']['StringValue']))
                    continue;
                $this->_receipts_handlers[$msg['MessageId']] = $msg['ReceiptHandle'];
                $commands = array(
                    'messageId' => $msg['MessageId'],
                    'command' => $msg['MessageAttributes']['command']['StringValue'],
                    'params' => unserialize($msg['Body'])
                );
            }
            // CakeLog::debug(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Returning Commands ' . print_r($commands, true));
            return $commands;
        }
        return $messages;
    }

    public function finishCommand($messageId, $prio = 'normal')
    {
        $deleteResult = $this->deleteMessage($this->_queue_urls[$prio], $this->_receipts_handlers[$messageId])->toArray();
        if ($deleteResult['@metadata']['statusCode'] == 200) {
            // CakeLog::debug(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Removing ' . $messageId);
            unset($this->_receipts_handlers[$messageId]);
            return true;
        }
        return false;
    }

    /**
     *
     * @param string $queue_url
     * @param array $messageAttributes
     * @param string $messageBody
     * @param boolean $dedupProtect
     * @return \Aws\Result|boolean
     */
    public function sendMessage(string $queue_url, array $messageAttributes, string $messageBody, $dedupProtect = false)
    {
        try {
            $messageGroupId = Configure::read('QueueDaemon.APP.' . $this->configApp . '.uuid');
            $params = array(
                "MessageGroupId" => $messageGroupId,
                "MessageDeduplicationId" => $dedupProtect === false ? \Ramsey\Uuid\Uuid::uuid4() : \Ramsey\Uuid\Uuid::uuid5($messageGroupId, $messageBody), // If deduplication is enabled generate an uuidv5 based on the messagroupid else generate a random uuidv4
                'MessageBody' => $messageBody,
                'QueueUrl' => $queue_url,
                'MessageAttributes' => $messageAttributes
            );
            return $this->AwsSqsClient->sendMessage($params);
        } catch (\Aws\Exception\AwsException $e) {
            CakeLog::error($e->getMessage());
            return false;
        }
    }

    /**
     *
     * @param string $queue_url
     * @param number $max_messages
     * @return \Aws\Result|NULL|boolean
     */
    public function readMessages(string $queue_url, $max_messages = 1)
    {
        try {
            $result = $this->AwsSqsClient->receiveMessage(array(
                'AttributeNames' => [
                    'SentTimestamp',
                    'MessageGroupId'
                ],
                'MaxNumberOfMessages' => $max_messages,
                'MessageAttributeNames' => [
                    'command'
                ],
                'QueueUrl' => $queue_url
            ));
            $messages = $result->get('Messages');
            if (count($messages) > 0) {
                return $messages;
            } else {
                // there aren't new messages return null
                return null;
            }
        } catch (\Aws\Exception\AwsException $e) {
            CakeLog::error($e->getMessage());
            return false;
        }
    }

    /**
     *
     * @param string $queue_url
     * @param string $receipt_handle
     * @return \Aws\Result|boolean
     */
    public function deleteMessage(string $queue_url, string $receipt_handle)
    {
        try {
            $result = $this->AwsSqsClient->deleteMessage([
                'QueueUrl' => $queue_url,
                'ReceiptHandle' => $receipt_handle
            ]);
            return $result;
        } catch (\Aws\Exception\AwsException $e) {
            CakeLog::error($e->getMessage());
            return false;
        }
        return true;
    }

    /**
     *
     * @param string $queue_name
     * @return \Aws\Result|boolean
     */
    public function getQueueUrl(string $queue_name)
    {
        try {
            $queue = $this->AwsSqsClient->getQueueUrl(array(
                'QueueName' => $queue_name
            ));
            return $queue->get('QueueUrl');
        } catch (\Aws\Exception\AwsException $e) {
            CakeLog::error($e->getMessage());
            return false;
        }
    }

    public function processJob($messageId, $command, $params, $priority)
    {
        CakeLog::error(__METHOD__ . ' processJob must be implemented in extended class without calling parent::processJob');
        die();
    }
}
