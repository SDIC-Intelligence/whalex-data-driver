package com.meiya.whalex.db.entity.file;

import com.meiya.whalex.db.entity.AbstractDbHandler;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import lombok.Builder;
import lombok.Data;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 操作实体
 *
 * @author wangkm1
 * @date 2025-03-31
 * @project whale-cloud-platformX
 */
@Data
public class FtpHandler extends AbstractDbHandler {

    private Query query;

    private DropTable dropTable;

    private CreateTable createTable;

    private AlterTable alterTable;
    
    private Insert insert;

    private MergeData mergeData;

    private QueryFileTreeNode queryFileTreeNode;

    @Data
    public static class MergeData {
        private String src;
        private long maxMergeSize = 10 * 1024 * 1024;

        /**
         * 合并的块大小
         */
        private Long blockSize;

        /**
         * 排除的文件
         */
        private List<String> excludeFiles;

        /**
         * 排除的目录
         */
        private List<String> excludePaths;

        /**
         * 执行的合并周期值，作用于分区类型HDFS表
         */
        private Integer exePeriodValue;

        /**
         * 小文件占比
         */
        private Double mergeScale;
    }


    @Data
    public static class AlterTable {
        private String newTableName;
    }

    @Data
    public static class DropTable {
        private Date startTime;
        private Date stopTime;
    }

    @Data
    @Builder
    public static class Query {
        private int limit;
    }

    @Data
    @Builder
    public static class CreateTable {
        private List<Group> parquetData;
        private MessageType parquetSchema;
        private StandardStructObjectInspector orcSchema;
        private List<Writable> orcData;
    }

    @Data
    @Builder
    public static class Insert {
        private List<Map<String, Object>> rowData;

        private Boolean commitNow;
    }

}
