package com.jadic.rdb;

import java.util.HashMap;
import java.util.Map;


/**
 * @author 	Jadic
 * @created 2014-5-22
 */
public class RDBRestore {
    
    private RDBParser rdbParser;
    private RDBSaver rdbSaver;

    /**
     * 
     * @param ip        redis server ip
     * @param port      redis server port
     * @param filePath  rdb file path
     */
    public RDBRestore(String ip, int port, String filePath) {
        rdbSaver = new RDBSaver(ip, port);
        rdbParser = new RDBParser(filePath, true, rdbSaver);
    }
    
    public void restore() {
        rdbSaver.start();
        rdbParser.start();
    }
    
    public static void main(String[] args) {
        Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
        map.put("1".getBytes(), "2".getBytes());
        Entry entry = new Entry();
        entry.setValue(map);
        
        RDBRestore rdbRestore = new RDBRestore("192.168.1.168", 6379, "E:/vm_shared/dump.rdb");
        rdbRestore.restore();
    }
}
