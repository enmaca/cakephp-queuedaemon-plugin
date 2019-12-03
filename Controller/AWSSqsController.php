<?php
require_once CakePlugin::path('QueueDaemon') . 'Vendor/autoload.php';
App::uses('Controller', 'Controller');

class AWSSqsController extends Controller
{

    public $AwsSqsClient = null;

    public $configApp = 'default';

    public $baseClass = '';

    public $viewClass = 'Json';

    public $visibilitySQSTimeOut = 3600 * 4;

    protected $_mergeParent = 'AWSSqsController';

    public function __construct($request = null, $response = null)
    {
        parent::__construct($request, $response);
        if (! Configure::read('QueueDaemon.AWS'))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.AWS] ');

        if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.queues'))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.queues] ');

        if (! Configure::read('QueueDaemon.APP.' . $this->configApp . '.uuid'))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.uuid] ');

        if (Configure::read('QueueDaemon.AWS.region') && Configure::read('QueueDaemon.AWS.version') && Configure::read('QueueDaemon.AWS.key_id') && Configure::read('QueueDaemon.AWS.key_secret'))
            $this->AwsSqsClient = \Aws\Sqs\SqsClient::factory(array(
                'region' => Configure::read('QueueDaemon.AWS.region'),
                'version' => Configure::read('QueueDaemon.AWS.version'),
                'credentials' => array(
                    'key' => Configure::read('QueueDaemon.AWS.key_id'),
                    'secret' => Configure::read('QueueDaemon.AWS.key_secret')
                )
            ));
    }

    /**
     *
     * @param string $command
     * @param array|string $params
     * @param string $prio
     * @param boolean $dedupProtect
     * @throws MethodNotAllowedException
     */
    public function enqueueCommand(string $command, $params = array(), $prio = 'normal', $dedupProtect = false)
    {
        if (empty($command))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Data [command] ');

        $queue_data = Configure::read('QueueDaemon.APP.' . $this->configApp);

        if (empty($queue_data['queues'][$prio]))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.queues' . $prio . ']');

        $queue_url = $this->getQueueUrl($queue_data['queues'][$prio]);

        $messageAttributes = array(
            "command" => array(
                'DataType' => "String",
                'StringValue' => $command
            )
        );

        if (! is_string($params))
            $messageBody = serialize($params);
        else
            $messageBody = $params;

        $sendResult = $this->sendMessage($queue_url, $messageAttributes, $messageBody, $dedupProtect);

        if (! empty($sendResult))
            $sendResult = $sendResult->toArray();

        $result = 'failed';
        if ($sendResult['@metadata']['statusCode'] == 200) {
            $result = 'ok';
            $LogModel = Configure::read('QueueDaemon.LogModel');
            if (! empty($LogModel)) {
                $this->loadModel($LogModel);
                $this->$LogModel->save([
                    'message_id' => $sendResult['MessageId'],
                    'params' => $messageBody,
                    'priority' => $prio
                ]);
            }
        }

        $this->set(array(
            'result' => $result,
            'data' => $result == 'ok' ? $sendResult['MessageId'] : array(),
            '_serialize' => array(
                'result',
                'data'
            )
        ));

        if ($this->autoRender === true) {
            $this->render();
            $this->response->send();
            $this->_stop();
        }
    }

    /**
     *
     * @param string $prio
     * @param number $max_messages
     * @throws MethodNotAllowedException
     */
    public function getQueuedCommands($prio = 'normal', $max_messages = 1)
    {
        $queue_data = Configure::read('QueueDaemon.APP.' . $this->configApp);
        if (empty($queue_data))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '] ');

        if (empty($queue_data['queues'][$prio]))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $this->configApp . '.queues.' . $prio . 'Missing] ');

        $queue_url = $this->getQueueUrl($queue_data['queues'][$prio]);

        $messages = $this->readMessages($queue_url, $max_messages);

        $commands = array();
        if (is_array($messages)) {
            foreach ($messages as $msg) {
                if (empty($msg['MessageAttributes']['command']['StringValue']))
                    continue;
                $commands = array(
                    'receiptHandle' => $msg['ReceiptHandle'],
                    'messageId' => $msg['MessageId'],
                    'command' => $msg['MessageAttributes']['command']['StringValue'],
                    'params' => unserialize($msg['Body'])
                );
            }
        }

        $this->set(array(
            'result' => $messages === false ? 'failed' : 'ok',
            'data' => ! empty($commands) ? $commands : $messages,
            '_serialize' => array(
                'result',
                'data'
            )
        ));

        if ($this->autoRender === true) {
            $this->render();
            $this->response->send();
            $this->_stop();
        }
    }

    public function finishCommand($receiptHandle, $prio = 'normal')
    {
        $queue_data = Configure::read('QueueDaemon.APP.' . $this->configApp);
        if (empty($queue_data))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $configApp . '] ');

        if (empty($queue_data['queues'][$prio]))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Configure [QueueDaemon.APP.' . $configApp . '.queues.' . $prio . 'Missing] ');

        $queue_url = $this->getQueueUrl($queue_data['queues'][$prio]);
        $deleteResult = $this->deleteMessage($queue_url, $receiptHandle);

        $result = 'failed';
        if (is_object($deleteResult))
            $result = $deleteResult['@metadata']['statusCode'] == 200 ? 'ok' : 'failed';

        $this->set(array(
            'result' => $result,
            'data' => array(),
            '_serialize' => array(
                'result',
                'data'
            )
        ));

        if ($this->autoRender === true) {
            $this->render();
            $this->response->send();
            $this->_stop();
        }
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
            print_r($e->getMessage());
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
                'VisibilityTimeout' => $this->visibilitySQSTimeOut,
                'MaxNumberOfMessages' => $max_messages,
                'MessageAttributeNames' => [
                    'command'
                ],
                'QueueUrl' => $queue_url
            ));
            $messages = $result->get('Messages');
            if (! empty($messages) && count($messages) > 0) {
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

    public function get()
    {
        /**
         * TODO: Implementar Bien.
         */
        $prio = 'normal';
        if (! empty($this->request->query('prio')))
            $prio = $this->request->query('prio');

        $max_messages = 1;
        if (! empty($this->request->query('commands')))
            $max_messages = $this->request->query('commands');

        $this->getQueuedCommands($prio, $max_messages);
    }

    public function post()
    {
        /**
         * Soporte para construccion de comando via Router (params)
         *
         * Router::connect('/:psubgroup/:pcommand', array( 'controller' => $controller, "action" => "post", "[method]" => "POST", 'version' => 'v1' ));
         *
         * @var unknown $_subgroup
         */
        $_subgroup = '';
        if (! empty($this->request->params['psubgroup']))
            $_subgroup = $this->request->params['psubgroup'] . '_';

        $_command = '';
        if (! empty($this->request->params['pcommand']))
            $_command = $this->request->params['pcommand'];

        $_paramCommand = $_subgroup . $_command;

        $prio = 'normal';
        if (! empty($this->request->query('prio')))
            $prio = $this->request->query('prio');

        if (empty($_paramCommand)) {
            if (empty($this->request->data['command']))
                throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Params  [command] ');

            if (! empty($this->request->data['subgroup']))
                $command = $this->request->data['subgroup'] . '_' . $this->request->data['command'];
            else
                $command = $this->request->data['command'];
        } else {
            $command = $_paramCommand;
        }

        $this->enqueueCommand($command, empty($this->request->data['params']) ? array() : $this->request->data['params'], $prio, empty($this->request->data['dedupProtection']) ? false : true);
    }

    public function delete()
    {
        if (empty($this->request->data['receiptHandle']))
            throw new MethodNotAllowedException(((Configure::read('debug') > 0) ? '[' . __METHOD__ . '] ' : '') . 'Missing Data  [receiptHandle] ');

        $prio = 'normal';
        if (! empty($this->request->query('prio')))
            $prio = $this->request->query('prio');

        $this->finishCommand($this->request->data['receiptHandle'], $prio);
    }
}
