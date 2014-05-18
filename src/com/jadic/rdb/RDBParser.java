package com.jadic.rdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.jadic.utils.KKTool;

/**
 * redis rdb file parser 
 * @author Jadic
 */
public class RDBParser {
	
	private RandomAccessFile raf;
	private ILoadNewEntry iLoadNewEntry;
	
	private final static byte[] RDB_FILE_HEAD = "redis".getBytes();
	
	public RDBParser(ILoadNewEntry iLoadNewEntry) {
		this.iLoadNewEntry = iLoadNewEntry;
	}
	
	private boolean isFileHeadValid() {
		byte[] head = new byte[5];
		if (readBytes(head) && KKTool.isByteArrayEqual(head, RDB_FILE_HEAD)) {
			byte[] verBuf = new byte[4];
			if (readBytes(verBuf)) {
				int ver = KKTool.bytes2Int(verBuf);
			}
		}
		return false;
	}
	
	/**
	 * verify checkSum 
	 * @return
	 */
	private boolean isCheckSumValid() {
		return true;
	}

	private Entry getNextEntry() {
		return null;
	}
	
	private boolean loadPair(Entry entry) {
		return true;
	}
	
	private boolean readBytes(byte[] buf) {
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
	
	public boolean parse(File rdbFile, boolean verifyCheckSum) throws FileNotFoundException {
		raf = new RandomAccessFile(rdbFile, "r");
		
		if (!isFileHeadValid()) {
			return false;
		}
		
		if (verifyCheckSum && !isCheckSumValid()) {
			return false;
		}
		
		Entry entry = null;
		while((entry = getNextEntry()) != null) {
			if (this.iLoadNewEntry != null) {
				this.iLoadNewEntry.loadNewEntry(entry);
			}
		}
		
		return true;
	}
	
	public static void main(String[] args) {
		
	}

}
