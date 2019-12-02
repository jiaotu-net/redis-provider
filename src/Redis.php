<?php

/**
 * reids操作类
 * @author haohui.wang
 */
namespace JiaoTu\RedisProvider;

class Redis {
    /**
     * Redis配置
     * 
     * @var array
     */
    protected $config = array ();
    
    /**
     * Redis对象
     *
     * @var \Redis
     */
    protected $handler = null;
    
    /**
     * 连接重试计数
     * @var integer
     */
    protected $retryCount = 0;
    
    /**
     * 自动重连间隔(微秒)
     * @var integer
     */
    protected $retryInterval = 0;
    
    /**
     * 构造
     * @param array $config 配置项, 配置参数:
     * string server 服务器地址
     * int port 服务器端口
     * int database 数据库编号
     * string prefix KEY前缀
     * string auth 授权
     * boolean pconnect 长连接
     * int serializer 序列化方式  Redis::SERIALIZER_ 开头的常量
     * int retry_interval 自动连接重试间隔毫秒数
     * int retry_conut 自动连接重试次数
     * float timeout 连接超时秒数
     * float read_timeout 读取超时秒数
     */
    public function __construct($config) {
        $this->config = $config;
        if (!isset($this->config ['pconnect'])) {
            $this->config ['pconnect'] = false;
        }
        
        if (!isset($this->config ['retry_conut'])) {
            $this->config ['retry_conut'] = 0;
        }
        
        if (isset($this->config ['retry_interval']) && $this->config ['retry_interval'] > 0) {
            $this->retryInterval = (int) ($this->config ['retry_interval'] * 1000);
        }
        
        unset ( $this->handler ); // 先干掉, 配合 __get 按需连接
    }
    
    public function & __call($name, $arguments) {
        try {
            $ret = call_user_func_array(array($this->handler, $name), $arguments);
        } catch (\RedisException $e) {
            if (false === $this->handler->ping()) {
                /* 自动断线重连处理 */
                while ($this->retryCount > 0) {
                    -- $this->retryCount;
                    if ($this->connect()) {
                        $this->retryCount = $this->config ['retry_conut'];
                        $ret = & $this->__call($name, $arguments);
                        return $ret;
                    }
                    
                    if ($this->retryInterval > 0) {
                        usleep($this->retryInterval);
                    }
                }
                
                if ($this->config ['pconnect'] && null !== $this->handler) {
                    $this->handler->close();
                }
                
                $this->handler = null;
                unset($this->handler);
            }
            
            throw $e;
        }
        return $ret;
    }
    
    /**
     * 加锁
     *
     * @param string $key
     *            锁的标识名
     * @param int $timeout
     *            获取锁失败时的等待超时时间(秒), 在此时间之内会一直尝试获取锁直到超时. 为 0 表示失败后直接返回不等待
     * @param int $expire
     *            当前锁的最大生存时间(秒), 必须大于 0 . 如果超过生存时间后锁仍未被释放, 则系统会自动将其强制释放
     * @param int $waitIntervalUs
     *            获取锁失败后挂起再试的时间间隔(微秒)
     * @throws \RedisException
     * @return boolean 成功返回true; 失败返回false.
     */
    public function lock($key, $timeout = 0, $expire = 15, $waitIntervalUs = 100000) {
        if (empty ( $key )) {
            return false;
        }
        
        $timeout = ( int ) $timeout;
        $expire = max ( ( int ) $expire, 5 );
        $now = microtime ( true );
        $timeoutAt = $now + $timeout;
        
        while ( true ) {
            if ($this->set($key, $now, array('nx', 'ex' => $expire))) {
                return true;
            }
            
            // 设置了不等待或者已超时
            if ($timeout <= 0 || microtime ( true ) > $timeoutAt) {
                break;
            }
            
            // 挂起一段时间再试
            usleep ( $waitIntervalUs );
        }
        
        return false;
    }
    
    /**
     * 解锁
     * @param string $key
     * @return int
     */
    public function unlock($key) {
        return $this->delete ( $key );
    }
    
    /**
     * 连接
     * @return boolean
     */
    public function connect() {
        $this->close();
        
        $this->handler = new \Redis ();

        if ( $this->config ['pconnect'] ) {
            if (! $this->handler->pconnect ( $this->config ['server'], isset ( $this->config ['port'] ) ? $this->config ['port'] : 6379, isset ( $this->config ['timeout'] ) ? $this->config ['timeout'] : 0, 'defalut', isset ( $this->config ['retry_interval'] ) ? $this->config ['retry_interval'] : 0, isset ( $this->config ['read_timeout'] ) ? $this->config ['read_timeout'] : 0 )) {
                $this->handler = null;
                unset ( $this->handler );
                return false;
            }
        } else {
            if (! $this->handler->connect ( $this->config ['server'], isset ( $this->config ['port'] ) ? $this->config ['port'] : 6379, isset ( $this->config ['timeout'] ) ? $this->config ['timeout'] : 0, null, isset ( $this->config ['retry_interval'] ) ? $this->config ['retry_interval'] : 0, isset ( $this->config ['read_timeout'] ) ? $this->config ['read_timeout'] : 0 )) {
                $this->handler = null;
                unset ( $this->handler );
                return false;
            }
        }
        
        if (isset ( $this->config ['auth'] )) {
            if (! $this->handler->auth ( $this->config ['auth'] )) {
                $this->close();
                return false;
            }
        }
        
        if (isset ( $this->config ['database'] )) {
            if (! $this->handler->select ( $this->config ['database'] )) {
                $this->close();
                return false;
            }
        }
        
        if (isset ( $this->config ['prefix'] )) {
            $this->handler->setOption ( \Redis::OPT_PREFIX, $this->config ['prefix'] );
        }
        
        if (isset ( $this->config ['serializer'] )) {
            $this->handler->setOption ( \Redis::OPT_SERIALIZER, $this->config ['serializer'] );
        }
        
        return true;
    }
    
    public function __get($name) {
        if ('handler' == $name) {
            if ($this->connect ()) {
                return $this->handler;
            }
            
            throw new \RedisException('Connection lost', 0);
        }
        
        return null;
    }
    
    public function close() {
        if (isset ( $this->handler )) {
            $this->handler->close ();
            $this->handler = null;
            unset ( $this->handler );
        }
    }
}