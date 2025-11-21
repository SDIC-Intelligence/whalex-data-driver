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
import com.meiya.whalex.db.module.file.BaseFtpServiceImpl;
import com.meiya.whalex.db.util.helper.impl.file.CustomFtpFileSystem;
import com.meiya.whalex.db.util.param.impl.file.FtpTableSchema;
import com.meiya.whalex.exception.BusinessException;
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
import java.net.URI;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * FileSystem 连接对象实体
 *
 * @author wangkm1
 * @date 2025/03/31
 * @project whale-cloud-platformX
 */
@Slf4j
public class FtpClient {

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


    public FtpClient() {
        //TODO 无参构造函数 方便其他子类继承
    }

    /**
     * 构造文件系统连接对象
     *
     * @param databaseInfo
     * @throws Exception
     */
    public FtpClient(FtpDatabaseInfo databaseInfo) throws Exception {
        conf = new Configuration();
        String hDfileSystemUri = databaseInfo.getServiceUrl();
        hDfileSystemUri = hDfileSystemUri.replaceAll("ftp://", "");
        hDfileSystemUri =databaseInfo.getUserName()+":"+ URLEncoder.encode(databaseInfo.getPassword(),"UTF-8") +"@"+hDfileSystemUri;
        conf.set("fs.defaultFS", "ftp://"+hDfileSystemUri);
        conf.set("fs.ftp.user.", databaseInfo.getUserName());
        conf.set("fs.ftp.password.", databaseInfo.getPassword());
        if(StringUtils.isNotBlank(databaseInfo.getControlEncoding())){
            conf.set("ftp.client.control-encoding",databaseInfo.getControlEncoding());
        }
        //打印配置信息
        if(log.isDebugEnabled()) {
            Iterator<Map.Entry<String, String>> iterator = conf.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                log.debug("Ftp 配置内容: key:[{}], value:[{}]", next.getKey(), next.getValue());
            }
        }

        log.info("Ftp 配置内容: [{}]", conf.toString());
        this.fileSystem = new CustomFtpFileSystem();
        fileSystem.initialize(URI.create( "ftp://"+hDfileSystemUri),conf);
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
     * 从文件中读取流
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
