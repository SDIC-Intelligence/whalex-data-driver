package com.meiya.whalex.db.entity.file;

import com.meiya.whalex.db.stream.StreamIterator;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

public class HadoopStreamIterator implements StreamIterator<byte[]> {

    private FSDataInputStream inputStream;

    private int len;

    private byte[] data = new byte[4096];

    public HadoopStreamIterator(FSDataInputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public boolean hasNext() {
        return len != -1;
    }

    @Override
    public byte[] next() {
        try {
            len = inputStream.read(data, 0, data.length);
            if(len == -1) {
                inputStream.close();
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    @Override
    public int getDataLength() {
        return len;
    }
}
