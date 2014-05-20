package com.jadic.rdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.jadic.utils.KKTool;

/**
 * redis rdb file parser
 * 
 * @author Jadic
 */
public class RDBParser {

    private Logger logger = Logger.getLogger(RDBParser.class);

    private ILoadNewEntry iLoadNewEntry;
    private RandomAccessFile raf;
    private int currentDBIndex;

    private final static byte[] RDB_FILE_HEAD = "redis".getBytes();

    private final static int REDIS_TYPE_STRING = 0;
    private final static int REDIS_TYPE_LIST = 1;
    private final static int REDIS_TYPE_SET = 2;
    private final static int REDIS_TYPE_ZSET = 3;
    private final static int REDIS_TYPE_HASH = 4;
    private final static int REDIS_TYPE_HASH_ZIPMAP = 9;
    private final static int REDIS_TYPE_LIST_ZIPLIST = 10;
    private final static int REDIS_TYPE_SET_INTSET = 11;
    private final static int REDIS_TYPE_ZSET_ZIPLIST = 12;
    private final static int REDIS_TYPE_HASH_ZIPLIST = 13;

    private final static int REDIS_TYPE_EXPIRE_SECONDS = 0xFC;// 252
    private final static int REDIS_TYPE_EXPIRE_MILLISECONDS = 0xFD;// 253
    private final static int REDIS_TYPE_SELECT_DB = 0xFE;// 254
    private final static int REDIS_TYPE_EOF = 0xFF;// 255

    private final static int REDIS_6BIT = 0;
    private final static int REDIS_14BIT = 1;
    private final static int REDIS_32BIT = 2;
    private final static int REDIS_ENCVAL = 3;

    private final static int GET_DB_INDEX_ERR = 0xFFFF;

    private final static int REDIS_GET_LENGTH_ERR = Integer.MAX_VALUE;

    public RDBParser(ILoadNewEntry iLoadNewEntry) {
        this.iLoadNewEntry = iLoadNewEntry;
        this.currentDBIndex = 0;
    }

    private boolean isFileHeadValid() {
        byte[] head = new byte[5];
        if (readBytes(head) && KKTool.isByteArrayEqual(head, RDB_FILE_HEAD)) {
            byte[] verBuf = new byte[4];
            if (readBytes(verBuf)) {
                int ver = KKTool.bytes2Int(verBuf);
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
                entry.setSuccess(raf.getFilePointer() + 8 < raf.length());
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
            entry.setSuccess(false);
        } else {
            entry.setSuccess(true);
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
                expireTime = KKTool.bytes2Int(buf) * 1000;
            } else {
                expireTime = KKTool.bytes2Long(buf);
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
            break;
        case REDIS_TYPE_HASH_ZIPMAP:
            break;
        case REDIS_TYPE_LIST_ZIPLIST:
            break;
        case REDIS_TYPE_SET_INTSET:
            break;
        case REDIS_TYPE_ZSET_ZIPLIST:
            break;
        case REDIS_TYPE_HASH_ZIPLIST:
            break;
        case REDIS_TYPE_LIST:
            break;
        case REDIS_TYPE_SET:
            break;
        case REDIS_TYPE_ZSET:
            break;
        case REDIS_TYPE_HASH:
            break;
        default:
            break;
        }
        return true;
    }

    private String loadKey() {
        try {
            int keyLen = raf.read();
            byte[] buf = new byte[keyLen];
            if (readBytes(buf)) {
                return new String(buf);
            }
        } catch (IOException e) {
        }
        return null;
    }

    private int loadLength(boolean[] iseconded) {
        byte[] buf = new byte[2];
        if (iseconded != null && iseconded.length > 0) {
            Arrays.fill(iseconded, false);
        }

        if (!readBytes(buf, 0, 1)) {
            return REDIS_GET_LENGTH_ERR;
        }
        int flag = (buf[0] & 0xC0) >>> 6;
        if (flag == REDIS_6BIT) {
            return buf[0] & 0x3F;
        } else if (flag == REDIS_ENCVAL) {
            iseconded[0] = true;
            return buf[0] & 0x3F;
        } else if (flag == REDIS_14BIT) {
            if (!readBytes(buf, 1, 1)) {
                return REDIS_GET_LENGTH_ERR;
            }
            return (buf[0] & 0x3F) << 8 | buf[1];
        } else {
            byte[] buf2 = new byte[4];
            if (!readBytes(buf2)) {
                return REDIS_GET_LENGTH_ERR;
            }
            return KKTool.bytes2Int(buf2, false);
        }
    }

    private String loadStringObject() {
        return null;
    }

    private String loadLZFStringObject() {
        return null;
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

    public boolean parse(File rdbFile, boolean verifyCheckSum) {
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
                        if (this.iLoadNewEntry != null) {
                            this.iLoadNewEntry.loadNewEntry(entry);
                        }
                    }
                } else {
                    parseResult = false;
                    logger.info("load entry fail, quit parse, entry:" + entry);
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

    public static void main(String[] args) {

    }

}
