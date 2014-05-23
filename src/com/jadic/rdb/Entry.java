package com.jadic.rdb;

import java.nio.charset.Charset;

public final class Entry {

    private int type;
    private byte[] key;
    private Object value;
    private long expireTime;// timeunit:milliseconds
    private int dbIndex;
    private boolean success;

    public Entry() {
        this.type = -1;
        this.key = null;
        this.value = null;
        this.expireTime = 0;
        this.dbIndex = 0;
        this.success = false;
    }
    
    public Entry getCopyEntry() {
        Entry entry = new Entry();
        entry.setType(this.type);
        entry.setKey(this.key);
        entry.setExpireTime(this.expireTime);
        entry.setDbIndex(this.dbIndex);
        entry.setSuccess(this.success);
        return entry;
    }

    public Entry(int dbIndex) {
        this();
        this.dbIndex = dbIndex;
    }

    public String toString() {
        StringBuilder sBuilder = new StringBuilder();
        sBuilder.append("type:").append(type).append(",key:").append(key);
        sBuilder.append(",value:").append(value).append(",expireTime:").append(expireTime);
        sBuilder.append(",dbNo:").append(dbIndex);
        return sBuilder.toString();
    }
    
    public String toStringWithoutValue() {
        StringBuilder sBuilder = new StringBuilder();
        sBuilder.append("type:").append(type).append(",key:").append(key);
        sBuilder.append(",expireTime:").append(expireTime);
        sBuilder.append(",dbNo:").append(dbIndex);
        return sBuilder.toString();
        
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public byte[] getKey() {
        return key;
    }
    
    public String getKeyStr() {
        return new String(key);
    }
    
    public String getKeyStr(String charset) {
        return new String(key, Charset.forName(charset));
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getValueWithType() {
        return (T)value;
    }
    
    public void setValue(Object value) {
        this.value = value;
    }

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public int getDbIndex() {
        return dbIndex;
    }

    public void setDbIndex(int dbIndex) {
        this.dbIndex = dbIndex;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

}
