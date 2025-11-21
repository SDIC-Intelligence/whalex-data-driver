package com.meiya.whalex.jdbc.bean;

public interface WatchableStream {

    void setWatcher(OutputStreamWatcher outputStreamWatcher);

    int size();

    byte[] toByteArray();

    void write(byte[] data, int off, int len);

}
