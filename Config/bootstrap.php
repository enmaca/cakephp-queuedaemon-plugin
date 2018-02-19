<?php
Configure::write('QueueDaemon', array(
    'AWS' => array(
        'region' => 'us-west-2',
        'version' => '2012-11-05',
        'key_id' => 'AWS_ACCESS_KEY_ID',
        'key_secret' => 'AWS_ACCESS_KEY_SECRET'
    )
));