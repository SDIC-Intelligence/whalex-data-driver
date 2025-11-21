package com.meiya.whalex.filesystem.module;

import com.meiya.whalex.db.entity.DatabaseSetting;
import com.meiya.whalex.db.entity.PageResult;
import com.meiya.whalex.db.entity.TableSetting;
import com.meiya.whalex.db.stream.StreamIterator;
import com.meiya.whalex.filesystem.entity.FileTreeNode;
import com.meiya.whalex.filesystem.entity.QueryFileTreeNode;
import com.meiya.whalex.filesystem.entity.UploadFileParam;

/**
 * 分布式文件系统统一抽象接口
 *
 * @author 黄河森
 * @date 2023/9/1
 * @package com.meiya.whalex.filesystem.module
 * @project whalex-data-driver
 * @description DistributedFileSystemService
 */
public interface DistributedFileSystemService {

    /**
     * 查询文件路径列表
     * @param queryFileTreeNode
     * @return
     * @throws Exception
     */
    FileTreeNode ll(DatabaseSetting databaseSetting, TableSetting tableSetting, QueryFileTreeNode queryFileTreeNode) throws Exception;

    /**
     * 查询文件路径列表
     * @return
     * @throws Exception
     */
    StreamIterator<byte[]> downFile(DatabaseSetting databaseSetting, TableSetting tableSetting, String path) throws Exception;

    /**
     * 上传文件
     * @return
     * @throws Exception
     */
    PageResult uploadFile(DatabaseSetting databaseSetting, TableSetting tableSetting, UploadFileParam uploadFileParam) throws Exception;

    /**
     * 创建目录
     *
     * @param databaseSetting
     * @param targetPath
     * @return
     * @throws Exception
     */
    boolean mkdirs(DatabaseSetting databaseSetting, String targetPath) throws Exception;

    /**
     * 判断路径是否存在
     *
     * @param databaseSetting
     * @param targetPath
     * @return
     * @throws Exception
     */
    boolean exists(DatabaseSetting databaseSetting, String targetPath) throws Exception;

    /**
     * 判断是否文件
     *
     * @param databaseSetting
     * @param targetPath
     * @return
     * @throws Exception
     */
    boolean isFile(DatabaseSetting databaseSetting, String targetPath) throws Exception;

    /**
     * 判断是否路径
     *
     * @param databaseSetting
     * @param targetPath
     * @return
     * @throws Exception
     */
    boolean isDirectory(DatabaseSetting databaseSetting, String targetPath) throws Exception;

    /**
     * 拷贝本地文件
     *
     * @param databaseSetting
     * @param src
     * @param dst
     * @param delSrc
     * @param overwrite
     * @throws Exception
     */
    void copyFromLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean overwrite) throws Exception;

    /**
     * 批量移动本地文件
     *
     * @param databaseSetting
     * @param srcList
     * @param dst
     * @param delSrc
     * @param overwrite
     * @throws Exception
     */
    void moveFromLocalFile(DatabaseSetting databaseSetting, String[] srcList, String dst, boolean delSrc, boolean overwrite) throws Exception;

    /**
     * 移动本地文件
     *
     * @param databaseSetting
     * @param src
     * @param dst
     * @param delSrc
     * @param overwrite
     * @throws Exception
     */
    void moveFromLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean overwrite) throws Exception;

    /**
     * 拷贝至本地
     *
     * @param databaseSetting
     * @param src
     * @param dst
     * @param delSrc
     * @param useRawLocalFileSystem
     * @throws Exception
     */
    void copyToLocalFile(DatabaseSetting databaseSetting, String src, String dst, boolean delSrc, boolean useRawLocalFileSystem) throws Exception;

}
