package com.jadic.rdb;


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
        RDBRestore rdbRestore = new RDBRestore("192.168.1.109", 6379, "E:/vm_shared/dump.rdb");
        rdbRestore.restore();
    }
}
