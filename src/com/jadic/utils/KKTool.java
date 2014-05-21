package com.jadic.utils;

public final class KKTool {

    private KKTool() {
    };

    /**
     * true:both buf1 and buf2 are not null and same length, each byte in the
     * array with same index is equal
     * 
     * @param buf1
     * @param buf2
     * @return
     */
    public static boolean isByteArrayEqual(byte[] buf1, byte[] buf2) {
        if (buf1 == null || buf2 == null || buf1.length != buf2.length) {
            return false;
        }

        for (int i = 0; i < buf1.length; i++) {
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
        for (int i = 0; i < len / 2; i++) {
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
        return bytes2Short(buf, 0, isBigEndian);
    }

    public static short bytes2Short(byte[] buf, int sIndex, boolean isBigEndian) {
        int val = 0;
        if (!isBytesEmpty(buf) && sIndex >= 0 && sIndex + 1 < buf.length) {
            if (isBigEndian) {
                val = (0x00ff & buf[sIndex]) << 8 | (0x00ff & buf[sIndex + 1]);
            } else {
                val = (0x00ff & buf[sIndex]) | (0x00ff & buf[sIndex + 1]) << 8;
            }
        }
        return (short) val;
    }

    public static int bytes2Int(byte[] buf, boolean isBigEndian) {
        return bytes2Int(buf, 0, isBigEndian);
    }

    public static int bytes2Int(byte[] buf, int sIndex, boolean isBigEndian) {
        int val = 0;
        if (!isBytesEmpty(buf) && sIndex >= 0 && sIndex + 3 < buf.length) {
            if (isBigEndian) {
                val = (0x00ff & buf[sIndex]) << 24 | (0x00ff & buf[sIndex + 1]) << 16 
                    | (0x00ff & buf[sIndex + 2]) << 8 | (0x00ff & buf[sIndex + 3]);
            } else {
                val = (0x00ff & buf[sIndex]) | (0x00ff & buf[sIndex + 1]) << 8 
                    | (0x00ff & buf[sIndex + 2]) << 16 | (0x00ff & buf[sIndex + 3]) << 24;
            }
        }
        return val;
    }

    public static long bytes2Long(byte[] buf, boolean isBigEndian) {
        return bytes2Long(buf, 0, isBigEndian);
    }

    public static long bytes2Long(byte[] buf, int sIndex, boolean isBigEndian) {
        long val = 0;
        if (!isBytesEmpty(buf) && sIndex >= 0 && sIndex + 7 < buf.length) {
            if (isBigEndian) {
                val = (0x00ff & buf[sIndex]) << 56 | (0x00ff & buf[sIndex + 1]) << 48 
                    | (0x00ff & buf[sIndex + 2]) << 40 | (0x00ff & buf[sIndex + 3]) << 32 
                    | (0x00ff & buf[sIndex + 4]) << 24 | (0x00ff & buf[sIndex + 5]) << 16 
                    | (0x00ff & buf[sIndex + 6]) << 8 | (0x00ff & buf[sIndex + 7]);
            } else {
                val = (0x00ff & buf[sIndex]) | (0x00ff & buf[sIndex + 1]) << 8 
                    | (0x00ff & buf[sIndex + 2]) << 16 | (0x00ff & buf[sIndex + 3]) << 24 
                    | (0x00ff & buf[sIndex + 4]) << 32 | (0x00ff & buf[sIndex + 5]) << 40 
                    | (0x00ff & buf[sIndex + 6]) << 48 | (0x00ff & buf[sIndex + 7]) << 56;
            }
        }
        return val;
    }

    public static boolean isBytesEmpty(byte[] buf) {
        return buf == null || buf.length == 0;
    }

    public static int getUnsignedByte(byte b) {
        return b >= 0 ? b : 256 + b;
    }
    
    public static byte[] int2Bytes(int iValue, boolean bigOrLittle) {
        byte[] b = new byte[4];
        if (bigOrLittle) {
            b[0] = (byte)(iValue >> 24);
            b[1] = (byte)(iValue >> 16);
            b[2] = (byte)(iValue >> 8);
            b[3] = (byte)(iValue);
        } else {
            b[0] = (byte)(iValue);
            b[1] = (byte)(iValue >> 8);
            b[2] = (byte)(iValue >> 16);
            b[3] = (byte)(iValue >> 24);
        }
        return b;
    }
    
    public static String byteToHexStr(byte data) {
        String s = Integer.toHexString(data & 0xFF); 
        return (s.length() == 2 ? s : "0" + s).toUpperCase();
    }

    public static String byteArrayToHexStr(byte[] data) {
        StringBuilder result = new StringBuilder("");
        for (int i = 0; i < data.length; i ++) {
            result.append(byteToHexStr(data[i]));
        }
        return result.toString();
    }


    public static void main(String[] args) {
        
    }

    public static void log(Object object) {
        System.out.println(object);
    }

}
