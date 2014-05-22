package com.jadic.rdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.jadic.utils.KKTool;

/**
 * @author 	Jadic
 * @created 2014-5-20
 */
public class ZSetZipList {

    private byte[] buf;
    private boolean isParsed;
    private Map<String, Double> elements;
    
    public ZSetZipList(byte[] ziplistBuf) {
        this.buf = ziplistBuf;
        this.isParsed = false;
        this.elements = new HashMap<String, Double>();
    }
    
    private void parseElements() {
        if (this.isParsed) {
            return;
        }
        this.isParsed = true;
        if (buf == null) {
            return;
        }
        
        int offset = 0;
        
        KKTool.bytes2Int(buf, offset, false);//zlbytes 
        offset += 4;
        
        KKTool.bytes2Int(buf, offset, false);//zltail
        offset += 4;
        
        int elementsCount = KKTool.bytes2Short(buf, offset, false);//zllen
        offset += 2;
        
        int prevLen = 0;
        int specialFlag = 0;
        int encoding = 0;
        int strLen;
        byte bVal;
        short sVal;
        int iVal;
        long lVal;
        String element;
        String member = null;
        for (int i = 0; i < elementsCount; i ++) {
            prevLen = buf[offset ++];
            if (prevLen == 0xFE) {
                offset += 4;//next 4 bytes are used to store length
            }
            
            specialFlag = buf[offset ++];
            encoding = (specialFlag & 0x00C0) >> 6;
            if (encoding == 0x00) {
                //|00pppppp| – 1 byte : String value with length less than or equal to 63 bytes (6 bits).
                strLen = specialFlag;
                element = new String(Arrays.copyOfRange(buf, offset, offset + strLen));
                offset += strLen;
            } else if (encoding == 0x01) {
                //|01pppppp|qqqqqqqq| – 2 bytes : String value with length less than or equal to 16383 bytes (14 bits).
                strLen = (specialFlag & 0x003F) << 8 | buf[offset ++];
                element = new String(Arrays.copyOfRange(buf, offset, offset + strLen));
                offset += strLen;
            } else if (encoding == 0x10) {
                //|10______|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| – 5 bytes.
                strLen = KKTool.bytes2Int(buf, offset, false);
                offset += 4;
                
                element = new String(Arrays.copyOfRange(buf, offset, offset + strLen));
                offset += strLen;
            } else {// if (encoding == 0x11)
                encoding = (specialFlag & 0x0030) >> 4;
                if (encoding == 0x00) {//|1100____| – Read next 2 bytes as a 16 bit signed integer
                    sVal = KKTool.bytes2Short(buf, offset, false);
                    offset += 2;
                    element = String.valueOf(sVal);
                } else if (encoding == 0x01) {//|1101____| – Read next 4 bytes as a 32 bit signed integer
                    iVal = KKTool.bytes2Int(buf, offset, false);
                    offset += 4;
                    element = String.valueOf(iVal);
                } else if (encoding == 0x10) {//|1110____| – Read next 8 bytes as a 64 bit signed integer    
                    lVal = KKTool.bytes2Long(buf, offset, false);
                    offset += 8;
                    element = String.valueOf(lVal);
                } else {// if (encoding == 0x11)
                    encoding = (specialFlag & 0x00FF);
                    if (encoding == 0x00) {//|11110000| – Read next 3 bytes as a 24 bit signed integer
                        iVal = (buf[offset ++] & 0x00ff) | (buf[offset ++] & 0x00ff) << 8 | (buf[offset ++] & 0x00ff) << 16;
                        element = String.valueOf(iVal);
                    } else if (encoding == 0x0E) {//|11111110| – Read next byte as an 8 bit signed integer
                        bVal = buf[offset ++];
                        element = String.valueOf(bVal);
                    } else {
                    //|1111xxxx| – (with xxxx between 0000 and 1101) immediate 4 bit integer. 
                    //Unsigned integer from 0 to 12. The encoded value is actually from 1 to 13 because 0000 and 1111 can not be used, 
                    //so 1 should be subtracted from the encoded 4 bit value to obtain the right value.
                        iVal = (specialFlag & 0x0F) - 1;
                        element = String.valueOf(iVal);
                    }
                }
            }
            
            if (member == null) {//member
                member = element;
            } else {             //score
                elements.put(member, Double.parseDouble(element));
                member = null;
            }
        }
    }
    
    public int getElementsCount() {
        this.parseElements();
        return elements.size();
    }
    
    /**
     * elements may be modified by caller
     * @return
     */
    public Map<String, Double> getElements() {
        return elements;
    }

}
