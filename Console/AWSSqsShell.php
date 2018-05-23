<?php
App::uses('QueueDaemonShell', 'QueueDaemon.Console');
require_once CakePlugin::path('QueueDaemon') . 'Vendor/autoload.php';

class AWSSqsShell extends QueueDaemonShell
{

    public $AwsSqsClient = null;

    public $configApp = 'default';

    public $monitQueueDelay = 500000;

    public $baseClass = '';

    public $maxFork = array();

    public $max_processes = 1;

    public $queue_priorities = array(
        'high',
        'normal',
        'low'
    );

    public $forkedPIDS = array();

    private $_queue_urls = array();

    private $_receipts_handlers = array();

    private $_valid_methods = array();

    protected $jobs = array();

    public $uses = array();

    public function startup()
    {
        parent::startup();
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

        if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.max_processes')) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.max_processes] ');
            die();
        }

        if (!empty(Configure::read('QueueDaemon.APP.' . $this->configApp . '.monit_queue_delay'))) {
            $this->monitQueueDelay = Configure::read('QueueDaemon.APP.' . $this->configApp . '.monit_queue_delay');
        }else{
          $this->monitQueueDelay = 300000;
        }

        // $EventManager = self::staticLoadModel('EventManager');

        $this->AwsSqsClient = \Aws\Sqs\SqsClient::factory(array(
            'region' => Configure::read('QueueDaemon.AWS.region'),
            'version' => Configure::read('QueueDaemon.AWS.version'),
            'credentials' => array(
                'key' => Configure::read('QueueDaemon.AWS.key_id'),
                'secret' => Configure::read('QueueDaemon.AWS.key_secret')
            )
        ));

        $this->max_processes = Configure::read('QueueDaemon.APP.' . $this->configApp . '.max_processes');

        foreach ($this->queue_priorities as $prio) {
            if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio)) {
                CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio . ']');
                die();
            }
            $queue_url = $this->getQueueUrl(Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio));
            if (empty($queue_url)) {
                CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Queue in AWS Platform [' . Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio) . ']');
                die();
            }
            CakeLog::info(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Mapping queue [' . Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio) . '] => [' . $queue_url . ']');
            $this->_queue_urls[$prio] = $queue_url;
        }

        if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.methods')) {
            CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.methods]');
            die();
        }

        $this->maxFork = Configure::read('QueueDaemon.APP.' . $this->configApp . '.methods');

        foreach ($this->maxFork as $baseSQSCommand => $baseSQSCommandData) {
            if (is_array($baseSQSCommandData)) {
                /**
                 * $this is a subgroup command files
                 */
                $baseGroupFile = APP . 'Console' . DS . 'Command' . DS . Inflector::camelize($this->configApp) . DS . $baseSQSCommand;
                foreach ($baseSQSCommandData as $_baseSQSCommand => $dummy) {
                    $calledCommand = $baseSQSCommand . '::' . Inflector::underscore($_baseSQSCommand);
                    $class_file = $baseGroupFile . DS . $_baseSQSCommand . '.php';
                    if (! file_exists($class_file)) {
                        CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Method File [' . $class_file . ' for ' . $calledCommand . ']');
                        die();
                    }
                    CakeLog::info(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Loading File  [' . $class_file . ']');
                    require $class_file;

                    if (! is_callable(array(
                        $this->baseClass . $baseSQSCommand . $_baseSQSCommand,
                        'process'
                    ))) {
                        CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Can Not Call Method [' . $this->baseClass . $baseSQSCommand . $_baseSQSCommand . '::process]');
                        die();
                    }

                    $this->_valid_methods[$calledCommand] = array(
                        $this->baseClass . $baseSQSCommand . $_baseSQSCommand,
                        'process'
                    );
                }
            } else {
                $calledCommand = Inflector::underscore($baseSQSCommand);
                $class_file = APP . 'Console' . DS . 'Command' . DS . Inflector::camelize($this->configApp) . DS . $baseSQSCommand . '.php';
                if (! file_exists($class_file)) {
                    CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Method File [' . $class_file . ' for ' . $_ucmethod);
                    die();
                }

                require $class_file;

                if (! is_callable(array(
                    $this->baseClass . $baseSQSCommand,
                    'process'
                ))) {
                    CakeLog::error(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Can Not Call Method [' . $this->baseClass . @$_ucmethod . '::process]');
                    die();
                }

                $this->_valid_methods[$calledCommand] = array(
                    $this->baseClass . $baseSQSCommand,
                    'process'
                );
            }
        }
        // foreach ($this->_valid_methods as $apiMethod => $calledMethod) {
        //     CakeLog::info(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Valid Api Request [' . $apiMethod . '] Method to Call [' . print_r($calledMethod, true) . ']');
        // }
    }

    /**
     */
    public function monitQueue()
    {
        foreach ($this->queue_priorities as $priority)
            $this->jobs[$priority] = array();
        while (true) {

            $this->cleanChilds();
            if( count($this->_receipts_handlers) < $this->max_processes ){

              foreach ($this->queue_priorities as $priority) {
                  // $job = $this->getQueuedCommands($priority);
                  $job = $this->getQueuedCommands($priority);

                  if (! empty($job)) {
                      $this->jobs[$priority][] = $job;
                      break;
                  }
              }

                reset($this->queue_priorities);
                $jobDispatched = false;
                $jobForkedProcess = false;
                foreach ($this->queue_priorities as $priority) {
                  if (count($this->jobs[$priority]) > 0) {
                    foreach ($this->jobs[$priority] as $idx => $command_data) {

                      $jobForkedProcess = false;
                      $jobForkedProcess = $this->processJob($command_data['messageId'], array(
                        $this->baseClass . Inflector::camelize($command_data['command']),
                        'process'
                      ), $command_data['params'], $priority);

                      if ($jobForkedProcess != false)
                          $this->forkedPIDS[] = $jobForkedProcess;

                      $jobDispatched = true;
                      unset($this->jobs[$priority][$idx]);
                      $this->cleanChilds();
                    }
                    // if we found
                    break;
                  }
                }
                if ($jobDispatched)
                    continue;

            }
            // else{
              usleep($this->monitQueueDelay);
            //   $this->cleanChilds();
            // }

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
            CakeLog::debug(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Sendind Command ' . $command . ' [' . $sendResult['MessageId'] . ']');
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

                if (empty($msg['Body']))
                    $params = array();

                $params = @unserialize($msg['Body']);

                if ($params === FALSE)
                    $params = $msg['Body'];

                $commands = array(
                    'messageId' => $msg['MessageId'],
                    'command' => $msg['MessageAttributes']['command']['StringValue'],
                    'params' => $params
                );
            }
            // CakeLog::debug(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Returning Commands ' . print_r($commands, true));
            return $commands;
        }
        return $messages;
    }

    public function cleanChilds()
    {
        if (! is_array($this->forkedPIDS) || count($this->forkedPIDS) == 0) {
            return 0;
        }
        foreach ($this->forkedPIDS as $idx => $pdata) {
            $status = null;
            $pid = pcntl_waitpid($pdata['pid'], $status, WNOHANG);


            if ($pid > 0) {
                $this->out(__METHOD__ . " The '$pid' has been exited with code $status!!!");
                unset($this->forkedPIDS[$idx]);

                $errorProcess = false;
                if ($status == 0) {

                    $deleteResult = $this->deleteMessage($this->_queue_urls[$pdata['priority']], $this->_receipts_handlers[$pdata['messageId']]);
                    if(is_object($deleteResult)){
                      $deleteResult = $deleteResult->toArray();
                    }


                    if ( $deleteResult['@metadata']['statusCode'] != 200 ) {
                        // CakeLog::debug(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Removing ' . $pdata['messageId']);
                        // print_r($pdata);
                        $errorProcess = true;
                    }
                }else{
                  $errorProcess = true;
                }
                if($errorProcess){
                  // $EventManager = self::staticLoadModel($this->logModel);
                  $EventManager = self::staticLoadModel('EventManager');

                  $EventManagerLogToSave = array();
                  $EventManagerLogToSave[$this->logModel]['exited_code'] = $status;
                  $EventManagerLogToSave[$this->logModel]['message_body'] = serialize($pdata);
                  $EventManagerLogToSave[$this->logModel]['command'] = serialize($pdata['command']);
                  try {
                    $EventManager->create();
                    $EventManager->save($EventManagerLogToSave);
                    $EventManager->clear();

                  } catch (\Exception $e) {
                    print_r($e);
                  }


                }
                unset($this->_receipts_handlers[$pdata['messageId']]);
                return true;
            }
        }
        return count($this->forkedPIDS);
    }

    public function finishCommand($messageId, $prio = 'normal')
    {
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
            // CakeLog::error($e->getStatusCode());
            // Status code 400 is bad request (Reason: The receipt handle has expired).
            return array('@metadata' => array('statusCode' => $e->getStatusCode()));
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

    public function processJob($messageId, $callable_command, $params, $priority)
    {
        // CakeLog::info(__METHOD__ . ' MessageId :' . $messageId . ' Priority:' . $priority . ' command:' . print_r($callable_command) . ' Params:[' . print_r($params, true) . ']');
        // if (array_key_exists($callable_command, $this->maxFork))
        //     return self::multiProcessJob($messageId, $callable_command, $params, $priority);

        if (is_callable($callable_command))
            return array(
                'pid' => self::forkProcess($callable_command, $params),
                'messageId' => $messageId,
                'priority' => $priority,
                'command' => $callable_command
            );
        // $this->finishCommand($messageId, $priority);
        else
            // CakeLog::warning(__METHOD__ . ' Method not found [' . print_r($callable_command, true) . ']');
            return false;
    }

    public function multiProcessJob($messageId, $command, $params, $priority)
    {
    /**
     * $command = Inflector::camelize($command);
     *
     * CakeLog::info(__METHOD__ . ' MessageId :' . $messageId . ' Priority:' . $priority . ' Command:' . $command . ' Params:[' . print_r($params, true) . ']');
     * if (is_callable(array(
     * $this->baseClass . $command,
     * 'process'
     * ))) {
     * call_user_func(array(
     * $this->baseClass . $command,
     * 'process'
     * ), $params);
     * } else
     * CakeLog::warning(__METHOD__ . ' Method not found [' . $command . ']');
     */
    }

    /**
     * Check if a string is serialized
     *
     * @param string $string
     */
    public static function is_serialized($string)
    {
        return (@unserialize($string) !== false || $string == 'b:0;');
    }
}
