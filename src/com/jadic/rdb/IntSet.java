package com.jadic.rdb;

import java.util.HashSet;
import java.util.Set;

import com.jadic.utils.KKTool;

/**
 * @author 	Jadic
 * @created 2014-5-20
 */
public class IntSet {
    
    private byte[] buf;
    private boolean isParsed;
    private Set<byte[]> elements;
    
    public IntSet(byte[] intSetBytes) {
        this.buf = intSetBytes;
        elements = new HashSet<byte[]>(); 
    }
    
    /**
     * Within this string, the Intset has a very simple layout :<br>
     * <encoding><length-of-contents><contents>   <br>
     * encoding : is a 32 bit unsigned integer. It has 3 possible values – 2, 4 or 8. 
     *            It indicates the size in bytes of each integer stored in contents. 
     * length-of-contents : is a 32 bit unsigned integer, and indicates the length of the contents array
     * contents : is an array of $length-of-contents bytes. It contains the binary tree of integers
     */
    private void parseElements() {
        if (this.isParsed) {
            return;
        }
        this.isParsed = true;
        if (buf == null) {
            return;
        }
        int offset = 0;
        int encoding = KKTool.bytes2Int(buf, offset, false);
        offset += 4;
        
        int elementsCount = KKTool.bytes2Int(buf, offset, false);
        offset += 4;
        long element = 0;
        for (int i = 0; i < elementsCount && offset + encoding <= buf.length; i ++, offset += encoding) {
            if (encoding == 2) {
                element = KKTool.bytes2Short(buf, offset, false);
            } else if (encoding == 4){
                element = KKTool.bytes2Int(buf, offset, false);
            } else {
                element = KKTool.bytes2Long(buf, offset, false);
            }
            elements.add(String.valueOf(element).getBytes());
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
    public Set<byte[]> getElements() {
        this.parseElements();
        return elements;
    }
}
