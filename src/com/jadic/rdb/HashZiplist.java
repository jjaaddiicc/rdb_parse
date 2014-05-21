package com.jadic.rdb;

/**
 * @author 	Jadic
 * @created 2014-5-21
 */
public class HashZiplist extends ZSetZipList {

    public HashZiplist(byte[] ziplistBuf) {
        super(ziplistBuf);
    }

}
