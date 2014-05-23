package com.jadic.rdb;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.jadic.utils.MyExceptionHandler;

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
        byte[] sVal = null;
        List<byte[]> sList = null;
        Set<byte[]> set = null;
        Map<byte[], Double> zMap = null;
        Map<byte[], byte[]> sMap = null;
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
            for (byte[] val : sList) {
                pipeline.rpush(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_SET:         
            sList = entry.getValueWithType();
            for (byte[] val : sList) {
                pipeline.sadd(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_ZSET:        
            zMap = entry.getValueWithType();
            if (zMap.size() > 0) {
                Set<Map.Entry<byte[], Double>> entrySet = zMap.entrySet();
                for(Map.Entry<byte[], Double> e : entrySet) {
                    pipeline.zadd(entry.getKey(), e.getValue(), e.getKey());
                }
            }
            break;
        case RDBParser.REDIS_TYPE_HASH:     
            bytesMap = entry.getValueWithType();
            if (bytesMap.size() > 0) {
                pipeline.hmset(entry.getKey(), bytesMap);
            }
            break;
        case RDBParser.REDIS_TYPE_HASH_ZIPMAP: 
            //TODO // Zipmap encoding are deprecated starting Redis 2.6.
            break;
        case RDBParser.REDIS_TYPE_LIST_ZIPLIST:
            sList = entry.getValueWithType();
            for (byte[] val : sList) {
                pipeline.rpush(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_SET_INTSET:  
            set = entry.getValueWithType();
            for (byte[] val : set) {
                pipeline.sadd(entry.getKey(), val);
            }
            break;
        case RDBParser.REDIS_TYPE_ZSET_ZIPLIST:
            zMap = entry.getValueWithType();
            if (zMap.size() > 0) {
                Set<Map.Entry<byte[], Double>> entrySet = zMap.entrySet();
                for(Map.Entry<byte[], Double> e : entrySet) {
                    pipeline.zadd(entry.getKey(), e.getValue(), e.getKey());
                }
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
