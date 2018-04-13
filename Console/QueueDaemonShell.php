<?php
App::uses('Shell', 'Console');

class QueueDaemonShell extends Shell
{

    /**
     *
     * @return integer pid
     */
    public static function daemonize()
    {
        $child = pcntl_fork();
        if ($child)
            exit(0);
        posix_setsid();
        chdir('/');
        umask(0);
        return posix_getpid();
    }

    /**
     *
     * @param array $callable_function
     * @param array $params
     * @return number|unknown
     */
    public static function forkProcess(array $callable_function, $params = array())
    {
        $pid = pcntl_fork();
        $process_key = strtolower(__METHOD__) . '_' . $pid;
        if ($pid === - 1) {
            CakeLog::error("Can't Fork " . print_r($callable_function, true));
            return - 1;
        } else {
            if ($pid) {
                CakeLog::info("Forking new process with pid '$pid' and function '" . print_r($callable_function, true) . "'");
                return $pid;
            } else {
                if (! is_callable($callable_function)) {
                    CakeLog::warning("Can't Fork Process because '" . print_r($callable_function, true) . "' is not callable");
                    exit(- 1);
                }
                exit(call_user_func($callable_function, $params));
            }
        }
    }

    /**
     *
     * @param array $dispatched_process
     * @return array
     */
    public function waitPIDs(array & $dispatched_process)
    {
        if (! is_array($dispatched_process) || count($dispatched_process) == 0)
            return array();
        $exited_processes = array();
        foreach ($dispatched_process as $pid => $identifier) {
            $status = null;
            $pid = pcntl_waitpid($pid, $status, WNOHANG);
            if ($pid > 0) {
                CakeLog::info("The '$pid' has been exited with code $status");
                $exited_processes[$pid] = array(
                    'id' => $identifier,
                    'exit_code' => $status
                );
                unset($dispatched_process[$idx]);
            }
        }
        return $exited_processes;
    }
}