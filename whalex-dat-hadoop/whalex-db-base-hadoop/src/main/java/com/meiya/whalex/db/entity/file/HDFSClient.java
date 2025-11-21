package com.meiya.whalex.db.entity.file;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.meiya.whalex.cache.DatCaffeine;
import com.meiya.whalex.db.kerberos.KerberosEnvConfigurationUtil;
import com.meiya.whalex.db.kerberos.KerberosJaasConfigurationUtil;
import com.meiya.whalex.db.kerberos.KerberosLoginCallBack;
import com.meiya.whalex.db.kerberos.KerberosUniformAuth;
import com.meiya.whalex.db.kerberos.exception.LoginKerberosException;
import com.meiya.whalex.db.module.file.BaseHadoopServiceImpl;
import com.meiya.whalex.db.util.param.impl.file.HdfsTableSchema;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.ItemFieldTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.*;

import java.io.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * FileSystem 连接对象实体
 *
 * @author 黄河森
 * @date 2019/12/30
 * @project whale-cloud-platformX
 */
@Slf4j
public class HDFSClient {

    private static final String logIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";

    private static final String logIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String logIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String logIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String logIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    /**
     * 文件系统操作对象
     */
    protected FileSystem fileSystem;

    /**
     * 用户名（认证体系下使用）
     */
    protected String userName;

    /**
     * 配置信息
     */
    protected Configuration conf;

    /**
     * 当前 HDFS 中存在的 parquet 结构缓存
     */
    protected Cache<String, SchemaInfo<MessageType>> PARQUET_SCHEMA_CACHE = DatCaffeine.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();;

    /**
     * 当前 HDFS 中存在的 orc 结构缓存
     */
    protected Cache<String, SchemaInfo<StandardStructObjectInspector>> ORC_SCHEMA_CACHE = DatCaffeine.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();;


    public HDFSClient() {
        //TODO 无参构造函数 方便其他子类继承
    }

    /**
     * 构造文件系统连接对象
     *
     * @param databaseInfo
     * @throws Exception
     */
    public HDFSClient(HadoopDatabaseInfo databaseInfo, KerberosUniformAuth kerberosUniformLogin) throws Exception {
        conf = new Configuration();
        String hDfileSystemUri = databaseInfo.getServiceUrl();
        hDfileSystemUri = hDfileSystemUri.replaceAll("hdfs://", "");
        String[] nameNodeUris = hDfileSystemUri.split("[,;]");
        String[] nameNodes;
        // 节点名称
        if (StringUtils.isNotBlank(databaseInfo.getNameNodes())) {
            nameNodes = StringUtils.split(databaseInfo.getNameNodes(), ",");
        } else {
            String haPre = "nn";
            nameNodes = new String[nameNodeUris.length];
            for (int i = 0; i < nameNodeUris.length; i++) {
                nameNodes[i] = haPre + i;
            }
        }
        // 集群名称
        String clusterName = StringUtils.isBlank(databaseInfo.getClusterName()) ? "hacluster" : databaseInfo.getClusterName();

        if (nameNodeUris.length > 1) {
            //多Node
            conf.set("fs.defaultFS", "hdfs://" + clusterName);
            conf.set("dfs.nameservices", clusterName);
            conf.set("dfs.ha.namenodes." + clusterName, getNnString(nameNodes));
            for (int i = 0; i < nameNodeUris.length; i++) {
                conf.set("dfs.namenode.rpc-address." + clusterName + "." + nameNodes[i], nameNodeUris[i]);
            }
            conf.set("dfs.client.failover.proxy.provider." + clusterName, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        } else {
            //单个nameNode
            conf.set("fs.defaultFS", "hdfs://" + hDfileSystemUri);
        }
        // 设置 fs.hdfs.impl
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",  org.apache.hadoop.fs.LocalFileSystem.class.getName());
        if (kerberosUniformLogin != null) {
            // 认证内容
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hadoop.security.authorization", "true");
            conf.set("dfs.namenode.kerberos.principal.pattern", "*");
            conf.set("hadoop.security.token.service.use_ip", "true");
            if (StringUtils.isNotBlank(kerberosUniformLogin.getKerberosPrincipal())) {
                conf.set("hbase.kerberos.principal", kerberosUniformLogin.getKerberosPrincipal());
                conf.set("hadoop.http.kerberos.internal.spnego.principal", kerberosUniformLogin.getKerberosPrincipal());
            }
            if (StringUtils.isNoneBlank(databaseInfo.getRpcProtection())) {
                String rpcProtection = databaseInfo.getRpcProtection();
                if (!StringUtils.equals(rpcProtection, "authentication")
                        && !StringUtils.equals(rpcProtection, "integrity")
                        && !StringUtils.equals(rpcProtection, "privacy")) {
                    log.error("HBase 参数配置异常, rpcProtection: [{}] 无法识别", rpcProtection);
                    throw new BusinessException(ExceptionCode.DB_PARAM_ERROR_EXCEPTION);
                }
                conf.set("hbase.rpc.protection", databaseInfo.getRpcProtection());
            }else {
                conf.set("hadoop.rpc.protection", "privacy");
            }
            try {
                // 证书登录
                kerberosUniformLogin.login(KerberosJaasConfigurationUtil.Module.CLIENT, new KerberosLoginCallBack() {
                    @Override
                    public void loginExtend(KerberosUniformAuth kerberosUniformLogin) throws Exception {
                        KerberosEnvConfigurationUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
                        UserGroupInformation.setConfiguration(conf);
                        try {
                            UserGroupInformation.loginUserFromKeytab(kerberosUniformLogin.getUserName(), kerberosUniformLogin.getUserKeytabPath());
                        } catch (IOException e) {
                            log.error("login failed with " + kerberosUniformLogin.getUserName() + " and " + kerberosUniformLogin.getUserKeytabPath() + ".");
                            log.error("perhaps cause 1 is " + logIN_FAILED_CAUSE_PASSWORD_WRONG + ".");
                            log.error("perhaps cause 2 is " + logIN_FAILED_CAUSE_TIME_WRONG + ".");
                            log.error("perhaps cause 3 is " + logIN_FAILED_CAUSE_AES256_WRONG + ".");
                            log.error("perhaps cause 4 is " + logIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
                            log.error("perhaps cause 5 is " + logIN_FAILED_CAUSE_TIME_OUT + ".");
                            throw e;
                        }
                    }
                });
            } catch (IOException e) {
                throw new LoginKerberosException(e.getMessage(), e);
            }
            this.userName = kerberosUniformLogin.getUserName();
        }

        //打印配置信息
        if(log.isDebugEnabled()) {
            Iterator<Map.Entry<String, String>> iterator = conf.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                log.debug("HDFs 配置内容: key:[{}], value:[{}]", next.getKey(), next.getValue());
            }
        }

        log.info("HDFs 配置内容: [{}]", conf.toString());
        this.fileSystem = FileSystem.get(conf);
    }

    /**
     * 清除表结构缓存
     *
     * @param databaseInfo
     * @param tableInfo
     */
    public void invalidateSchemaCache(HadoopDatabaseInfo databaseInfo, HadoopTableInfo tableInfo) {
        String fileType = tableInfo.getFileType();
        if (StrUtil.equalsIgnoreCase(fileType, HadoopTableInfo.ORC_TYPE)) {
            ORC_SCHEMA_CACHE.invalidate(databaseInfo.getBashPath() + tableInfo.getPath() + "#" + tableInfo.getFileType());
        } else {
            PARQUET_SCHEMA_CACHE.invalidate(databaseInfo.getBashPath() + tableInfo.getPath() + "#" + tableInfo.getFileType());
        }
    }

    /**
     * 获取 Orc 结构
     *
     * @param tableInfo
     * @return
     */
    public SchemaInfo<StandardStructObjectInspector> getOrcSchema(HadoopDatabaseInfo databaseInfo, HadoopTableInfo tableInfo) throws Exception {
        return ORC_SCHEMA_CACHE.get(databaseInfo.getBashPath() + tableInfo.getPath() + "#" + tableInfo.getFileType(), new Function<String, SchemaInfo<StandardStructObjectInspector>>() {
            @Override
            public SchemaInfo<StandardStructObjectInspector> apply(String key) {
                String[] split = StringUtils.split(key, "#");
                String path = split[0];
                String fileType = split[1];
                String schemaPath = path + BaseHadoopServiceImpl.TABLE_META_DIC + fileType;
                RecordReader rows = null;
                List<Map<String, String>> schema = new ArrayList<>();
                try {
                    Reader reader = OrcFile.createReader(fileSystem, new Path(schemaPath));
                    rows = reader.rows();
                    while (rows.hasNext()) {
                        OrcStruct next = (OrcStruct) rows.next(null);
                        Map<String, String> resultMap = new HashMap<>(5);
                        Method getFieldValue = OrcStruct.class.getDeclaredMethod("getFieldValue", int.class);
                        getFieldValue.setAccessible(true);
                        Text name = (Text) getFieldValue.invoke(next, 0);
                        Text type = (Text) getFieldValue.invoke(next, 1);
                        BooleanWritable isPartition = (BooleanWritable) getFieldValue.invoke(next, 3);
                        Text partitionFormat = (Text) getFieldValue.invoke(next, 4);
                        Text _path = (Text) getFieldValue.invoke(next, 5);
                        resultMap.put("name", name.toString());
                        resultMap.put("type", type.toString());
                        resultMap.put("isPartition", isPartition.toString());
                        resultMap.put("partitionFormat", partitionFormat.toString());
                        resultMap.put("path", _path.toString());
                        schema.add(resultMap);
                    }
                } catch (Exception e) {
                    throw new BusinessException("HDFS 获取 Orc schema 异常!", e);
                } finally {
                    if (rows != null) {
                        try {
                            rows.close();
                        } catch (IOException ioException) {
                            log.error("HDFS 关闭 Orc 读取流对象异常!", ioException);
                        }
                    }
                }
                if (CollectionUtil.isEmpty(schema)) {
                    throw new BusinessException("HDFS 获取 parquet 结构元数据异常!");
                }
                List<String> fields = new ArrayList<>(schema.size());
                List<ObjectInspector> types = new ArrayList<>(schema.size());
                SchemaInfo.SchemaInfoBuilder<StandardStructObjectInspector> builder = SchemaInfo.<StandardStructObjectInspector>builder();
                for (Map<String, String> map : schema) {
                    String name = map.get("name");
                    String type = map.get("type");
                    boolean isPartition = StringUtils.isNotBlank(map.get("isPartition")) ? Boolean.valueOf(map.get("isPartition")) : false;
                    String partitionFormat = map.get("partitionFormat");
                    String uriPath = map.get("path");
                    ItemFieldTypeEnum fieldTypeEnum = ItemFieldTypeEnum.findFieldTypeEnum(type);
                    ObjectInspector objectInspector = HdfsTableSchema.orcTypeConvert(fieldTypeEnum);
                    fields.add(name);
                    types.add(objectInspector);
                    if (isPartition) {
                        builder.path(uriPath)
                                .format(partitionFormat)
                                .partitionFiled(name);
                    }
                }
                StandardStructObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                        fields,
                        types
                );
                return builder.schema(inspector).build();
            }
        });
    }

    /**
     * 获取 Parquet 结构
     *
     * @param tableInfo
     * @return
     */
    public SchemaInfo<MessageType> getParquetSchema(HadoopDatabaseInfo databaseInfo, HadoopTableInfo tableInfo) {
        return PARQUET_SCHEMA_CACHE.get(databaseInfo.getBashPath() + tableInfo.getPath() + "#" + tableInfo.getFileType(), new Function<String, SchemaInfo<MessageType>>() {
            @Override
            public SchemaInfo<MessageType> apply(String key) {
                String[] split = StringUtils.split(key, "#");
                String path = split[0];
                String fileType = split[1];
                String schemaPath = path + BaseHadoopServiceImpl.TABLE_META_DIC + fileType;
                ParquetReader<Group> reader = null;
                List<Map<String, String>> schema = new ArrayList<>();
                try {
                    reader = ParquetReader.builder(new GroupReadSupport(), new Path(schemaPath)).withConf(conf).build();
                    SimpleGroup group = null;
                    GroupType groupType = null;
                    // 读取数据
                    while (true) {
                        try {
                            if (!((group = (SimpleGroup) reader.read()) != null)) break;
                        } catch (IOException e) {
                            throw new BusinessException("HDFS 获取 Parquet schema 异常!", e);
                        }
                        if (groupType == null) {
                            groupType = group.getType();
                        }
                        Map<String, String> resultMap = new HashMap<>(5);
                        for (int i = 0; i < groupType.getFieldCount(); i++) {
                            String fieldName = groupType.getFieldName(i);
                            String value = group.getValueToString(i, 0);
                            resultMap.put(fieldName, value);
                        }
                        schema.add(resultMap);
                    }
                } catch (IOException e) {
                    throw new BusinessException("HDFS 获取 Parquet schema 异常!", e);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            log.error("HDFS 关闭 Parquet 读取流对象异常!", e);
                        }
                    }
                }
                if (CollectionUtil.isEmpty(schema)) {
                    throw new BusinessException("HDFS 获取 parquet 结构元数据异常!");
                }
                List<Type> typeList = new ArrayList<>();
                SchemaInfo.SchemaInfoBuilder<MessageType> builder = SchemaInfo.<MessageType>builder();
                for (Map<String, String> map : schema) {
                    String name = map.get("name");
                    String type = map.get("type");
                    boolean isPartition = StringUtils.isNotBlank(map.get("isPartition")) ? Boolean.valueOf(map.get("isPartition")) : false;
                    String partitionFormat = map.get("partitionFormat");
                    String uriPath = map.get("path");
                    ItemFieldTypeEnum fieldTypeEnum = ItemFieldTypeEnum.findFieldTypeEnum(type);
                    Type convert = HdfsTableSchema.parquetTypeConvert(name, fieldTypeEnum);
                    typeList.add(convert);
                    if (isPartition) {
                        builder.path(uriPath)
                                .format(partitionFormat)
                                .partitionFiled(name);
                    }
                }
                builder.schema(new MessageType(tableInfo.getTableName(), typeList));
                return builder.build();
            }
        });
    }

    /**
     * 解析节点名
     *
     * @param nameNodes
     * @return
     */
    protected String getNnString(String[] nameNodes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nameNodes.length; i++) {
            if (i == 0) {
                sb.append(nameNodes[i]);
            } else {
                sb.append("," + nameNodes[i]);
            }
        }
        return sb.toString();
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * 判定是否是目录
     *
     * @param path
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public boolean isDirectory(String path) throws IllegalArgumentException, IOException {
        return fileSystem.isDirectory(new Path(path));
    }

    /**
     * 判定是否是目录
     *
     * @param path
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public boolean isFile(String path) throws IllegalArgumentException, IOException {
        return fileSystem.isFile(new Path(path));
    }

    /**
     * 判断文件是否存在
     *
     * @param path
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public boolean exists(String path) throws IllegalArgumentException, IOException {
        return fileSystem.exists(new Path(path));
    }

    /**
     * 删除指定路径
     *
     * @param path
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public boolean delete(String path) throws IllegalArgumentException, IOException {
        return fileSystem.delete(new Path(path));
    }

    /**
     * 查看指定路径下的目录列表
     *
     * @param path
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<String> ls(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(new Path(path));
        List<String> paths = new ArrayList<>(fileStatus.length);
        for (FileStatus fileS : fileStatus) {
            paths.add(fileS.getPath().toString());
        }
        return paths;
    }

    /**
     * 查看指定路径下的目录列表
     *
     * @param path
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<FileStatus> ll(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
        return ll(new Path(path));
    }

    /**
     * 查看指定路径下的目录列表
     *
     * @param path
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<FileStatus> ll(Path path) throws FileNotFoundException, IllegalArgumentException, IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(path);
        return Arrays.asList(fileStatus);
    }

    /**
     * 查询文件内容（不限制读取行数）
     *
     * @param path
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<String> cat(String path) throws IllegalArgumentException, IOException {
        return cat(path, -1);
    }

    /**
     * 查询文件内容（限制读取行数）
     *
     * @param path
     * @param limit
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<String> cat(String path, int limit) throws IllegalArgumentException, IOException {
        return cat(path, -1, null);
    }

    /**
     * 查询文件内容
     *
     * @param path  文件路径
     * @param limit 限制读取行数
     * @param lines 保存实体
     * @return
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<String> cat(String path, int limit, List<String> lines) throws IllegalArgumentException, IOException {
        if (lines == null) {
            lines = new ArrayList<>();
        }
        int i = 0;
        BufferedReader br = null;
        DataInputStream input = null;
        try {
            input = fileSystem.open(new Path(path));
            br = new BufferedReader(new InputStreamReader(input));
            String line = br.readLine();
            while (line != null) {
                if (++i > limit && limit > 0) {
                    break;
                }
                lines.add(line);
                line = br.readLine();
            }
        } finally {
            if (br != null) {
                br.close();
            }
            if (input != null) {
                input.close();
            }
        }
        return lines;
    }

    /**
     * 统计文件行数
     *
     * @param path
     * @param limit
     * @return
     * @throws IOException
     */
    public long countLines(String path, long limit) throws IOException {
        if (isDirectory(path)) {
            long total = 0;
            FileStatus[] fileStatus = fileSystem.listStatus(new Path(path));
            if (null != fileStatus) {
                for (FileStatus fileSystem : fileStatus) {
                    if (fileSystem.isDirectory()) {
                        total += countLines(fileSystem.getPath().toString(), limit - total);
                        if (total >= limit) {
                            return limit;
                        }
                    } else {
                        if (fileSystem.getLen() <= 0) {
                            continue;
                        } else {
                            total += countLines(fileSystem.getPath().toString(), limit - total);
                            if (total >= limit) {
                                return limit;
                            }
                        }
                    }
                }
            }
            return total;
        } else {
            long i = 0;
            BufferedReader br = null;
            DataInputStream input = null;
            try {
                input = fileSystem.open(new Path(path));
                br = new BufferedReader(new InputStreamReader(input));
                String line = br.readLine();
                while (line != null) {
                    if (++i > limit && limit > 0) {
                        break;
                    }
                    line = br.readLine();
                }
            } finally {
                if (br != null) {
                    br.close();
                }
                if (input != null) {
                    input.close();
                }
            }
            return i;
        }
    }

    /**
     * 扫描文件系统文件
     *
     * @param fileSystemDirectory
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<String> catPartFiles(String fileSystemDirectory) throws FileNotFoundException, IllegalArgumentException, IOException {
        return catPartFiles(fileSystemDirectory, -1);
    }

    /**
     * 查看某个fileSystem目录下面的part文件
     *
     * @param fileSystemDirectory
     * @param limit
     * @return
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public List<String> catPartFiles(String fileSystemDirectory, int limit) throws FileNotFoundException, IllegalArgumentException, IOException {
        List<String> lines = new ArrayList<>();
        FileStatus[] fileStatus = fileSystem.listStatus(new Path(fileSystemDirectory));
        for (FileStatus fileS : fileStatus) {
            //没有文件内容的时候直接不要打开文件，可以明显提高速度
            if (fileS.getLen() <= 0) {
                continue;
            }
            Path hdfileSystemPath = fileS.getPath();
            if (hdfileSystemPath.getName().startsWith("part-")) {
                if (limit <= 0) {
                    cat(hdfileSystemPath.toString(), 0, lines);
                } else {
                    cat(hdfileSystemPath.toString(), limit - lines.size(), lines);
                    if (lines.size() >= limit) {
                        break;
                    }
                }
            }
        }
        return lines;
    }

    /**
     * 创建路径
     *
     * @param path
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public void mkdir(String path) throws IllegalArgumentException, IOException {
        fileSystem.mkdirs(new Path(path));
    }

    /**
     * 删除
     *
     * @param path
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public void rm(String path) throws IllegalArgumentException, IOException {
        fileSystem.delete(new Path(path), true);
    }

    /**
     * 写入数据
     *
     * @param toPath
     * @param lines
     * @throws IOException
     */
    public void writeLinesToHDFS(String toPath, Collection<String> lines) throws IOException {
        BufferedWriter bw = null;
        try{
            FSDataOutputStream fos = fileSystem.create(new Path(toPath));
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (String line : lines) {
                fos.write((line + "\n").getBytes("UTF-8"));
            }
            bw.flush();
        }finally {
            if(bw != null)  bw.close();
        }
    }

    /**
     * 向HDFS中写入bytes
     *
     * @param path
     * @param bytes
     * @throws IOException
     */
    public void writeBytesToHDFS(String path, byte[] bytes) throws IOException {
        FSDataOutputStream fos = null;
        try {
            fos = fileSystem.create(new Path(path));
            fos.write(bytes);
            fos.flush();
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    /**
     * weic 从HDFS读取byte[]
     *
     * @param path
     * @return
     * @throws IOException
     */
    public byte[] readBytesFromHDFS(String path) throws IOException {
        DataInputStream is = null;
        try {
            is = fileSystem.open(new Path(path));
            byte[] bytes = IOUtils.toByteArray(is);
            return bytes;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    /**
     * 从hadoop中读取流
     *
     * @param path
     * @return
     * @throws IOException
     */
    public FSDataInputStream open(Path path) throws IOException {
        return fileSystem.open(path);
    }

    /**
     * 重命名
     *
     * @param fromPath
     * @param toPath
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public void rename(String fromPath, String toPath) throws IllegalArgumentException, IOException {
        fileSystem.rename(new Path(fromPath), new Path(toPath));
    }

    /**
     * 将本地的文件拷贝到hdfileSystem上面
     * <p>
     * win提交的时候会出现权限问题
     * hadoop fileSystem -chmod 777 /spark/data
     */
    public void copyFromLocal(String fromPath, String toPath) throws IllegalArgumentException, IOException {
        fileSystem.copyFromLocalFile(new Path(fromPath), new Path(toPath));
    }

    /**
     * 将hdfileSystem的文件拷贝到本地
     */
    public void copyToLocalFile(String fromPath, String toPath) throws IllegalArgumentException, IOException {
        fileSystem.copyToLocalFile(false, new Path(fromPath), new Path(toPath), true);
    }

    /**
     * 获取文件大小
     *
     * @param dstPath
     * @return
     * @throws IOException
     */
    public long getSize(String dstPath) throws IOException {
        Path path = new Path(dstPath);
        if (!fileSystem.isDirectory(path)) {
            return fileSystem.getLength(path);
        }
        long len = 0;
        FileStatus[] fileStatus = fileSystem.listStatus(path);
        for (FileStatus fileSystem : fileStatus) {
            len += fileSystem.getLen();
        }
        return len;
    }
}
