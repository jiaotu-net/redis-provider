<?php
namespace JiaoTu\RedisProvider;

/**
 * 
 * @author haohui.wang
 *
 */
class RedisAdapter extends Redis {
    /**
     * 
     * @var \Redis
     */
    protected $adapter = null;
    
    public function __construct(& $redisAdapter, $config = []) {
        $this->adapter = $redisAdapter;
        if (!isset($config['server'])) {
            $config['server'] = '127.0.0.1';
        }
        parent::__construct($config);
    }
    
    /**
     * 连接
     * @return boolean
     */
    public function connect() {
        $this->close();
        
        $this->handler = $this->adapter;
        
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
}