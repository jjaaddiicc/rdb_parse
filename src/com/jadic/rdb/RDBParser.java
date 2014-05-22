package com.jadic.rdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.jadic.utils.KKTool;
import com.jadic.utils.LZF;
import com.jadic.utils.MyExceptionHandler;

/**
 * redis rdb file parser
 * 
 * @author Jadic
 */
public class RDBParser extends Thread {

    private Logger logger = Logger.getLogger(RDBParser.class);

    private IRestoreRDB iRestoreRDB;
    private RandomAccessFile raf;
    private File rdbFile;
    private boolean verifyCheckSum;
    private int currentDBIndex;

    private final static byte[] RDB_FILE_HEAD = "REDIS".getBytes();

    public final static int REDIS_TYPE_STRING              = 0;
    public final static int REDIS_TYPE_LIST                = 1;
    public final static int REDIS_TYPE_SET                 = 2;
    public final static int REDIS_TYPE_ZSET                = 3;
    public final static int REDIS_TYPE_HASH                = 4;
    public final static int REDIS_TYPE_HASH_ZIPMAP         = 9;
    public final static int REDIS_TYPE_LIST_ZIPLIST        = 10;
    public final static int REDIS_TYPE_SET_INTSET          = 11;
    public final static int REDIS_TYPE_ZSET_ZIPLIST        = 12;
    public final static int REDIS_TYPE_HASH_ZIPLIST        = 13;

    private final static int REDIS_TYPE_EXPIRE_MILLISECONDS = 0xFC;// 252
    private final static int REDIS_TYPE_EXPIRE_SECONDS      = 0xFD;// 253
    private final static int REDIS_TYPE_SELECT_DB           = 0xFE;// 254
    private final static int REDIS_TYPE_EOF                 = 0xFF;// 255

    private final static int REDIS_6BIT   = 0;
    private final static int REDIS_14BIT  = 1;
    private final static int REDIS_32BIT  = 2;
    private final static int REDIS_ENCVAL = 3;
    
    private final static int REDIS_RDB_ENC_INT8  = 0;/* 8 bit signed integer */
    private final static int REDIS_RDB_ENC_INT16 = 1;/* 16 bit signed integer */
    private final static int REDIS_RDB_ENC_INT32 = 2;/* 32 bit signed integer */
    private final static int REDIS_RDB_ENC_LZF   = 3;/* string compressed with FASTLZ */

    private final static int GET_DB_INDEX_ERR = 0xFFFF;

    private final static int REDIS_GET_LENGTH_ERR = Integer.MAX_VALUE;
    
    private final static int MAX_ELEMENTS = 10000;

    public RDBParser(String rdbFilePath, boolean verifyCheckSum, IRestoreRDB iLoadNewEntry) {
        this.iRestoreRDB = iLoadNewEntry;
        this.currentDBIndex = 0;
        this.rdbFile = new File(rdbFilePath);
        this.verifyCheckSum = verifyCheckSum;
        this.setUncaughtExceptionHandler(new MyExceptionHandler()); 
    }

    private boolean isFileHeadValid() {
        byte[] head = new byte[5];
        if (readBytes(head) && KKTool.isByteArrayEqual(head, RDB_FILE_HEAD)) {
            byte[] verBuf = new byte[4];
            if (readBytes(verBuf)) {
                String ver = new String(verBuf);
                logger.info("rdb ver:" + ver);
                return true;
            }
        }
        return false;
    }

    /**
     * verify checkSum
     * 
     * @return
     */
    private boolean isCheckSumValid() {
        return true;
    }

    private Entry loadEntry() {
        Entry entry = new Entry(this.currentDBIndex);
        if (!loadType(entry)) {
            return entry;
        }

        int type = entry.getType();
        if (type == REDIS_TYPE_EOF) {
            try {
                entry.setSuccess(raf.getFilePointer() + 8 <= raf.length());
            } catch (IOException e) {
                logger.error("");
            }
            return entry;
        } else if (type == REDIS_TYPE_SELECT_DB) {
            int dbIndex = getDBIndex();
            if (dbIndex == GET_DB_INDEX_ERR) {
                return entry;
            }

            if (dbIndex > 63) {
                logger.error("db index out range:" + dbIndex);
                return entry;
            }

            this.currentDBIndex = dbIndex;
            entry.setSuccess(true);
        } else {
            if (type == REDIS_TYPE_EXPIRE_SECONDS || type == REDIS_TYPE_EXPIRE_MILLISECONDS) {
                if (!processTime(entry)) {
                    return entry;
                }

                if (!loadType(entry)) {
                    return entry;
                }
            }
            if (!loadPair(entry)) {
                return entry;
            }
        }

        int nextType = peekType();
        if (isTypeValid(nextType)) {
            entry.setSuccess(true);
        } else {
            entry.setSuccess(false);
        }
        return entry;
    }

    private boolean loadType(Entry entry) {
        byte[] buf = new byte[1];
        if (readBytes(buf)) {
            int type = 0x00FF & buf[0];
            if (isTypeValid(type)) {
                entry.setType(type);
                return true;
            }
        }
        return false;
    }

    private boolean isTypeValid(int type) {
        switch (type) {
        case REDIS_TYPE_STRING:
        case REDIS_TYPE_LIST:
        case REDIS_TYPE_SET:
        case REDIS_TYPE_ZSET:
        case REDIS_TYPE_HASH:
        case REDIS_TYPE_HASH_ZIPMAP:
        case REDIS_TYPE_LIST_ZIPLIST:
        case REDIS_TYPE_SET_INTSET:
        case REDIS_TYPE_ZSET_ZIPLIST:
        case REDIS_TYPE_HASH_ZIPLIST:
        case REDIS_TYPE_EXPIRE_SECONDS:
        case REDIS_TYPE_EXPIRE_MILLISECONDS:
        case REDIS_TYPE_SELECT_DB:
        case REDIS_TYPE_EOF:
            return true;
        default:
            return false;
        }
    }

    private int getDBIndex() {
        byte[] buf = new byte[1];
        if (readBytes(buf)) {
            return KKTool.getUnsignedByte(buf[0]);
        } else {
            return GET_DB_INDEX_ERR;
        }
    }

    private boolean processTime(Entry entry) {
        int timeLen = entry.getType() == REDIS_TYPE_EXPIRE_SECONDS ? 4 : 8;
        byte[] buf = new byte[timeLen];
        if (readBytes(buf)) {
            long expireTime = 0;
            if (buf.length == 4) {
                expireTime = KKTool.bytes2Int(buf, false) * 1000L;
            } else {
                expireTime = KKTool.bytes2Long(buf, false);
            }
            entry.setExpireTime(expireTime);
            return true;
        }
        return false;
    }

    private int peekType() {
        byte[] buf = new byte[1];
        if (readBytes(buf)) {
            try {
                raf.seek(raf.getFilePointer() - buf.length);
                return KKTool.getUnsignedByte(buf[0]);
            } catch (IOException e) {
            }
        }
        return -1;
    }

    private boolean loadPair(Entry entry) {
        String key = loadKey();
        if (key == null) {
            return false;
        }
        entry.setKey(key);

        int len = 0;
        int type = entry.getType();
        if (type == REDIS_TYPE_LIST || type == REDIS_TYPE_SET || type == REDIS_TYPE_ZSET || type == REDIS_TYPE_HASH) {
            if ((len = loadLength(null)) == REDIS_GET_LENGTH_ERR) {
                return false;
            }
        }

        switch (type) {
        case REDIS_TYPE_STRING:
            entry.setValue(loadStringObject());
            if (entry.getValue() == null) {
                return false;
            }
            break;
        case REDIS_TYPE_HASH_ZIPMAP:
            // Zipmap encoding are deprecated starting Redis 2.6.
            return false;
        case REDIS_TYPE_LIST_ZIPLIST:
            byte[] zipListBytes = loadBytesObject();
            if (zipListBytes == null) {
                return false;
            }
            ZipList zipList = new ZipList(zipListBytes);
            List<String> elements = new ArrayList<String>();
            elements.addAll(zipList.getElements());
            entry.setValue(elements);
            break;
        case REDIS_TYPE_SET_INTSET:
            byte[] intSetBytes = loadBytesObject();
            if (intSetBytes == null) {
                return false;
            }
            IntSet intSet = new IntSet(intSetBytes);
            Set<String> intSetElements = new HashSet<String>();
            intSetElements.addAll(intSet.getElements());
            entry.setValue(intSetElements);
            break;
        case REDIS_TYPE_ZSET_ZIPLIST:
            byte[] zsetZiplistBytes = loadBytesObject();
            if (zsetZiplistBytes == null) {
                return false;
            }
            ZSetZipList zSetZipList = new ZSetZipList(zsetZiplistBytes);
            Map<String, Double> zSetZipListElements = new HashMap<String, Double>();
            zSetZipListElements.putAll(zSetZipList.getElements());
            entry.setValue(zSetZipListElements);
            break;
        case REDIS_TYPE_HASH_ZIPLIST:
            byte[] hashZListBytes = loadBytesObject();
            if (hashZListBytes == null) {
                return false;
            }
            HashZiplist hashZList = new HashZiplist(hashZListBytes);
            Map<String, String> hashZListElements = new HashMap<String, String>();
            hashZListElements.putAll(hashZList.getElements());
            entry.setValue(hashZListElements);
            break;
        case REDIS_TYPE_LIST:
            List<String> list1 = new ArrayList<String>();
            String s1 = null;
            for (int i = 0; i < len; i ++) {
                s1 = loadStringObject();
                if (s1 == null) {
                    return false;
                }
                list1.add(s1);
                
                if (list1.size() >= MAX_ELEMENTS) {
                    entry.setValue(list1);
                    restoreEntry(entry);
                    list1 = new ArrayList<String>();
                }                
            }
            entry.setValue(list1);
            break;
        case REDIS_TYPE_SET:
            List<String> list2 = new ArrayList<String>();
            String s2 = null;
            for (int i = 0; i < len; i ++) {
                s2 = loadStringObject();
                if (s2 == null) {
                    return false;
                }
                list2.add(s2);
                
                if (list2.size() >= MAX_ELEMENTS) {
                    entry.setValue(list2);
                    restoreEntry(entry);
                    list2 = new ArrayList<String>();
                }
            }
            entry.setValue(list2);
            break;
        case REDIS_TYPE_ZSET:
            Map<String, Double> zset = new HashMap<String, Double>();
            String member = null;
            String s3 = null;
            for (int i = 0; i < len * 2; i ++) {
                s3 = loadStringObject();
                if (s3 == null) {
                    return false;
                }
                if (member == null) {
                    member = s3;
                } else {
                    zset.put(member, Double.parseDouble(s3));
                    member = null;
                    
                    if (zset.size() >= MAX_ELEMENTS) {//avoid out of memory
                        entry.setValue(zset);
                        restoreEntry(entry);
                        zset = new HashMap<String, Double>();
                    }
                }
            }
            entry.setValue(zset);
            break;
        case REDIS_TYPE_HASH:
            Map<byte[], byte[]> hash = new HashMap<byte[], byte[]>();
            byte[] field = null;
            byte[] s4 = null;
            for (int i = 0; i < len * 2; i ++) {
                s4 = loadBytesObject();
                if (s4 == null) {
                    return false;
                }
                if (field == null) {
                    field = s4;
                } else {
                    hash.put(field, s4);
                    field = null;
                    
                    if (hash.size() >= MAX_ELEMENTS) {//avoid out of memory
                        entry.setValue(hash);
                        restoreEntry(entry);
                        hash = new HashMap<byte[], byte[]>();
                    }
                }
            }
            logger.info("hash.size:" + hash.size());
            entry.setValue(hash);
            break;
        default:
            break;
        }
        return true;
    }
    
    private String loadKey() {
        return loadStringObject();
    }

    private int loadLength(Encode encode) {
        byte[] buf = new byte[2];
        if (encode != null) {
            encode.setEncode(false);
        }

        if (!readBytes(buf, 0, 1)) {
            return REDIS_GET_LENGTH_ERR;
        }
        int flag = (buf[0] & 0xC0) >>> 6;
        if (flag == REDIS_6BIT) {
            return buf[0] & 0x3F;
        } else if (flag == REDIS_ENCVAL) {
            if (encode != null) {
                encode.setEncode(true);
            }
            return buf[0] & 0x3F;
        } else if (flag == REDIS_14BIT) {
            if (!readBytes(buf, 1, 1)) {
                return REDIS_GET_LENGTH_ERR;
            }
            return (buf[0] & 0x3F) << 8 | (buf[1] & 0x00FF);
        } else if (flag == REDIS_32BIT) {
            byte[] buf2 = new byte[4];
            if (!readBytes(buf2)) {
                return REDIS_GET_LENGTH_ERR;
            }
            return KKTool.bytes2Int(buf2, true);
        }
        return REDIS_GET_LENGTH_ERR;
    }
    
    private byte[] loadBytesObject() {
        Encode encode = new Encode();
        int len = loadLength(encode);
        if (len == REDIS_GET_LENGTH_ERR) {
            return null;
        }
        if (encode.isEncode()) {
            switch (len) {
            case REDIS_RDB_ENC_LZF:
                return loadLZFStringObjectBytes();
            default:
                return null;
            }
        }
        
        byte[] buf = new byte[len];
        if (!readBytes(buf)) {
            return null;
        }
        return buf;
    }

    private String loadStringObject() {
        Encode encode = new Encode();
        int len = loadLength(encode);
        if (len == REDIS_GET_LENGTH_ERR) {
            return null;
        }
        if (encode.isEncode()) {
            switch (len) {
            case REDIS_RDB_ENC_INT8:
            case REDIS_RDB_ENC_INT16:
            case REDIS_RDB_ENC_INT32:
                return loadIntegerObject(len);
            case REDIS_RDB_ENC_LZF:
                return loadLZFStringObject();
            default:
                return null;
            }
        }
        
        byte[] buf = new byte[len];
        if (!readBytes(buf)) {
            return null;
        }
        return new String(buf);
    }
    
    private String loadIntegerObject(int encodeType) {
        byte[] buf = new byte[4];
        long val = 0;
        switch (encodeType) {
        case REDIS_RDB_ENC_INT8:
            if (!readBytes(buf, 0, 1)) {
                return null;
            }
            val = buf[0];//can't use: val = 0x00FF & buf[0] because integer is saved in signed format
            break;
        case REDIS_RDB_ENC_INT16:
            if (!readBytes(buf, 0, 2)) {
                return null;
            }
            val = buf[0] | buf[1] << 8;
            break;
        case REDIS_RDB_ENC_INT32:
            if (!readBytes(buf)) {
                return null;
            }
            val = buf[0] | buf[1] << 8 | buf[2] << 16 | buf[3] << 24;
            break;
        default:
            logger.info("invalid encodeType:" + encodeType);
            return null;
        }
        return String.valueOf(val);
    }

    private String loadLZFStringObject() {
        int sLen = 0;//original string len
        int cLen = 0;//compressed string len
        if ((cLen = loadLength(null)) == REDIS_GET_LENGTH_ERR) {
            return null;
        }
        if ((sLen = loadLength(null)) == REDIS_GET_LENGTH_ERR) {
            return null;
        }
        
        byte[] cBuf = new byte[cLen];
        if (!readBytes(cBuf)) {
            return null;
        }
        
        byte[] sBuf = new byte[sLen];
        LZF.decompress(cBuf, 0, cBuf.length, sBuf, 0, sBuf.length);
        return new String(sBuf);
    }
    
    private byte[] loadLZFStringObjectBytes() {
        int sLen = 0;//original string len
        int cLen = 0;//compressed string len
        if ((cLen = loadLength(null)) == REDIS_GET_LENGTH_ERR) {
            return null;
        }
        if ((sLen = loadLength(null)) == REDIS_GET_LENGTH_ERR) {
            return null;
        }
        
        byte[] cBuf = new byte[cLen];
        if (!readBytes(cBuf)) {
            return null;
        }
        
        byte[] sBuf = new byte[sLen];
        LZF.decompress(cBuf, 0, cBuf.length, sBuf, 0, sBuf.length);
        return sBuf;
    }
    
    private boolean readBytes(byte[] buf) {
        if (buf == null) {
            return false;
        }

        try {
            if (raf.getFilePointer() + buf.length > raf.length()) {
                return false;
            }

            raf.read(buf);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean readBytes(byte[] buf, int sIndex, int count) {
        if (buf == null) {
            return false;
        }

        try {
            if (raf.getFilePointer() + buf.length > raf.length()) {
                return false;
            }

            raf.read(buf, sIndex, count);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean parse() {
        try {
            raf = new RandomAccessFile(rdbFile, "r");
            if (!isFileHeadValid()) {
                return false;
            }

            if (verifyCheckSum && !isCheckSumValid()) {
                return false;
            }

            boolean parseResult = true;
            Entry entry = null;
            while ((entry = loadEntry()) != null) {
                if (entry.isSuccess()) {
                    int type = entry.getType();
                    if (type == REDIS_TYPE_EOF) {
                        logger.info("rdb parse finished");
                        break;
                    } else if (type == REDIS_TYPE_SELECT_DB) {
                        continue;
                    } else {
                        //logger.info("parse an entry:" + entry);
                        this.restoreEntry(entry);
                    }
                } else {
                    parseResult = false;
                    logger.info("load entry[" + entry + "] fail, quit parse");
                    break;
                }
            }
            
            return parseResult;
        } catch (FileNotFoundException e) {
            logger.error("file not found:" + e.getMessage());
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    logger.error("raf close err:" + e.getMessage());
                }
            }
        }
        return false;
    }
    
    private void restoreEntry(Entry entry) {
        if (this.iRestoreRDB != null) {
            this.iRestoreRDB.restoreEntry(entry);
            logger.info("restore entry:" + entry.getKey());
        }
    }
    
    @Override
    public void run() {
        parse();
        this.iRestoreRDB.finish();
    }
    
    private class Encode {
        boolean isEncode;

        public boolean isEncode() {
            return isEncode;
        }

        public void setEncode(boolean isEncode) {
            this.isEncode = isEncode;
        }
        
    }

}
