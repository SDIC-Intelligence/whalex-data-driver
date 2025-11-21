package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import lombok.Data;

import java.io.InputStream;
import java.util.List;

/**
 * MONGO 组件操作实体
 *
 * @author 黄河森
 * @date 2019/9/14
 * @project whale-cloud-platformX
 */
@Data
public class S3Handle extends AbstractDbHandler {
    private S3QueryOrDel s3QueryOrDel;

    private S3Insert s3Insert;

    private UploadFile uploadFile;

    private DownFile downFile;

    @Data
    public static class S3QueryOrDel {

        private List<String> keys;
    }

    /**
     * 数据插入
     */
    @Data
    public static class S3Insert {
        /**
         * 多文件
         */
        List<S3RecordInfo> records;
    }

    /**
     * 数据插入
     */
    @Data
    public static class UploadFile {
        /**
         * 多文件
         */
       private InputStream inputStream;

       private String key;
    }

    /**
     * 数据插入
     */
    @Data
    public static class DownFile {

        private String key;
    }


    /**
     * 数据更新
     */
    @Data
    public static class AniUpdate {
    }

}
