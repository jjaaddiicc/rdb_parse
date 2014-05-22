package com.jadic.rdb;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.jadic.utils.MyExceptionHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * @author 	Jadic
 * @created 2014-5-22
 */
public class RDBSaver extends Thread implements IRestoreRDB {
    
    private Jedis jedis;
    private Pipeline pipeline;
    private Queue<Entry> entries;
    private boolean isStopped;
    private int currDBIndex;
    
    public RDBSaver(String ip, int port) {
        jedis = new Jedis(ip, port);
        entries = new LinkedBlockingQueue<Entry>();
        isStopped = false;
        currDBIndex = 0;
        jedis.select(currDBIndex);
        pipeline = jedis.pipelined();
        this.setUncaughtExceptionHandler(new MyExceptionHandler()); 
    }

    @Override
    public void restoreEntry(Entry entry) {
        if (entry != null) {
            entries.offer(entry);
        }
    }
    
    @Override
    public void finish() {
        this.isStopped = true;
    }

    @Override
    public void run() {
        Entry entry = null;
        int i = 0;
        while (!isInterrupted()) {
            if (isStopped && entries.isEmpty()) {
                break;
            }
            i = 0;
            while ((entry = entries.poll()) != null) {
                saveEntry(entry);
                if (++ i > 50) {
                    pipeline.sync();
                    i = 0;
                }
            }
            if (i > 0) {
                pipeline.sync();
            }
            
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
    
    private void saveEntry(Entry entry) {
        String sVal = null;
        List<String> sList = null;
        Set<String> set = null;
        Map<String, Double> zMap = null;
        Map<String, String> sMap = null;
        Map<byte[], byte[]> bytesMap = null;
        
        if (entry.getDbIndex() != currDBIndex) {
            currDBIndex = entry.getDbIndex();
            pipeline.select(currDBIndex);
        }
        switch (entry.getType()) {
        case RDBParser.REDIS_TYPE_STRING:      
            sVal = entry.getValueWithType();
            pipeline.set(entry.getKey(), sVal);
            break;
        case RDBParser.REDIS_TYPE_LIST:        
            sList = entry.getValueWithType();
            for (String val : sList) {
                pipeline.lpush(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_SET:         
            sList = entry.getValueWithType();
            for (String val : sList) {
                pipeline.sadd(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_ZSET:        
            zMap = entry.getValueWithType();
            if (zMap.size() > 0) {
                pipeline.zadd(entry.getKey(), zMap);
            }
            break;
        case RDBParser.REDIS_TYPE_HASH:     
            bytesMap = entry.getValueWithType();
            if (bytesMap.size() > 0) {
                pipeline.hmset(entry.getKey().getBytes(), bytesMap);
            }
            break;
        case RDBParser.REDIS_TYPE_HASH_ZIPMAP: 
            //TODO // Zipmap encoding are deprecated starting Redis 2.6.
            break;
        case RDBParser.REDIS_TYPE_LIST_ZIPLIST:
            sList = entry.getValueWithType();
            for (String val : sList) {
                pipeline.lpush(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_SET_INTSET:  
            set = entry.getValueWithType();
            for (String val : set) {
                pipeline.sadd(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_ZSET_ZIPLIST:
            zMap = entry.getValueWithType();
            if (zMap.size() > 0) {
                pipeline.zadd(entry.getKey(), zMap);
            }
            break;
        case RDBParser.REDIS_TYPE_HASH_ZIPLIST:                
            sMap = entry.getValueWithType();
            if (sMap.size() > 0) {
                pipeline.hmset(entry.getKey(), sMap);
            }
            break;
        default:
            break;
        }
    }

}
