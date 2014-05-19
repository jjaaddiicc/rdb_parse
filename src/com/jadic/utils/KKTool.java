package com.jadic.utils;


public final class KKTool {
	
	private KKTool() {
	};
	
	/**
	 * true:both buf1 and buf2 are not null and same length,
	 * 		each byte in the array with same index is equal
	 * @param buf1
	 * @param buf2
	 * @return
	 */
	public static boolean isByteArrayEqual(byte[] buf1, byte[] buf2) {
		if (buf1 == null || buf2 == null || buf1.length != buf2.length) {
			return false;
		}
		
		for (int i = 0; i < buf1.length; i ++) {
			if (buf1[i] != buf2[i]) {
				return false;
			}
		}
		
		return true;
	}
	
	public static void bigLittleEndianRevert(byte[] buf) {
		if (buf == null || buf.length % 2 == 1) {
			return;
		}
		
		int len = buf.length;
		byte t;
		for (int i = 0;i < len / 2; i ++) {
			t = buf[i];
			buf[i] = buf[len - 1 - i];
			buf[len - 1 - i] = t;
		}
		
	}
	
	public static short bytes2Short(byte[] buf) {
		return bytes2Short(buf, true);
	}
	
	public static int bytes2Int(byte[] buf) {
		return bytes2Int(buf, true);
	}
	
	public static long bytes2Long(byte[] buf) {
		return bytes2Long(buf, true);
	}
	
	public static short bytes2Short(byte[] buf, boolean isBigEndian) {
		short sVal = 0;
		if (!isBytesEmpty(buf) && buf.length <= 2) {
			for (int i = 0;i < buf.length; i ++) {
				sVal |= (buf[i] & 0xff) << (isBigEndian ? (buf.length - 1 - i) * 8 : i * 8);
			}
		}
		return sVal;
	}
	
	public static int bytes2Int(byte[] buf, boolean isBigEndian) {
		return 0;
	}
	
	public static long bytes2Long(byte[] buf, boolean isBigEndian) {
		return 0;
	}
	
	public static boolean isBytesEmpty(byte[] buf) {
		return buf == null || buf.length == 0;
	}
	
	public static int getUnsignedByte(byte b) {
        return b >= 0 ? b : 256 + b;
    }
	
	public static void main(String[] args) {
		byte[] buf = new byte[]{1, 2};
		log(bytes2Short(buf, false));
	}
	
	public static void log(Object object) {
		System.out.println(object);
	}

}
