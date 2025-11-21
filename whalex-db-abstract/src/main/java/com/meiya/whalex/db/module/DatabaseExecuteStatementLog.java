package com.meiya.whalex.db.module;

public class DatabaseExecuteStatementLog {

    private static final ThreadLocal<StringBuilder> THREAD_LOCAL = new ThreadLocal<>();

    public static void set(String statement) {

        StringBuilder builder = THREAD_LOCAL.get();
        if (builder == null) {
            builder = new StringBuilder();
            THREAD_LOCAL.set(builder);
        }

        if (builder.length() != 0) {
            builder.append("\n");
        }
        builder.append(statement);
    }


    public static String get() {
        StringBuilder builder = THREAD_LOCAL.get();
        if(builder == null) {
            return "";
        }
        return builder.toString();
    }

    /**
     * 清空数据
     */
    public static void remove() {
        THREAD_LOCAL.remove();
    }

}
