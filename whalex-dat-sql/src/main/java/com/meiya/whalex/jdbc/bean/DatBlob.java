package com.meiya.whalex.jdbc.bean;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class DatBlob implements Blob, OutputStreamWatcher{

    private byte[] data;

    public DatBlob(byte[] data) {
        this.data = data;
    }

    @Override
    public long length() throws SQLException {
        return data.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1L) {
            throw new RuntimeException("开始位置不能小于1");
        } else {
            --pos;
            if (pos > length()) {
                throw new RuntimeException("开始位置不能大于数组总长度");
            } else if (pos + (long)length > length()) {
                throw new RuntimeException("获取数组超出了范围");
            } else {
                byte[] newData = new byte[length];
                System.arraycopy(data, (int)pos, newData, 0, length);
                return newData;
            }
        }
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(data);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new RuntimeException("不支持此方法");
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(0, (int)pattern.length()), start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return this.setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {

        OutputStream outputStream = null;
        try {
            outputStream = setBinaryStream(pos);
            outputStream.write(bytes, offset, len);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(outputStream != null) {
                    outputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        if (pos < 1L) {
            throw new RuntimeException("开始位置不能小于1");
        } else {
            WatchableOutputStream bytesOut = new WatchableOutputStream();
            bytesOut.setWatcher(this);
            if (pos > 0L) {
                bytesOut.write(this.data, 0, (int)(pos - 1L));
            }

            return bytesOut;
        }
    }

    @Override
    public void truncate(long len) throws SQLException {
        if (len < 0L) {
            throw new RuntimeException("开始位置不能小于0");
        } else {
            if (len > length()) {
                throw new RuntimeException("开始位置不能大于数组总长度");
            } else {
                byte[] newData = new byte[(int)len];
                System.arraycopy(this.data, 0, newData, 0, (int)len);
                this.data = newData;
            }
        }
    }

    @Override
    public void free() throws SQLException {
        this.data = null;
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        if (pos < 1L) {
            throw new RuntimeException("开始位置不能小于1");
        } else {
            --pos;
            if (pos > length()) {
                throw new RuntimeException("开始位置不能大于数组总长度");
            } else if (pos + length > length()) {
                throw new RuntimeException("获取数组超出了范围");
            } else {
                return new ByteArrayInputStream(this.data, (int)pos, (int)length);
            }
        }
    }

    @Override
    public void streamClosed(WatchableStream out) {
        int streamSize = out.size();
        if (streamSize < this.data.length) {
            out.write(this.data, streamSize, this.data.length - streamSize);
        }

        this.data = out.toByteArray();
    }
}
