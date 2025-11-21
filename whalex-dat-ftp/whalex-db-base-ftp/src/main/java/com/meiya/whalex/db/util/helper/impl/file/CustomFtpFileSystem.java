package com.meiya.whalex.db.util.helper.impl.file;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.ftp.FTPException;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.fs.ftp.FTPInputStream;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;

public class CustomFtpFileSystem extends FileSystem {
    public static final Logger LOG = LoggerFactory.getLogger(FTPFileSystem.class);
    public static final int DEFAULT_BUFFER_SIZE = 1048576;
    public static final int DEFAULT_BLOCK_SIZE = 4096;
    public static final String FS_FTP_USER_PREFIX = "fs.ftp.user.";
    public static final String FS_FTP_HOST = "fs.ftp.host";
    public static final String FS_FTP_HOST_PORT = "fs.ftp.host.port";
    public static final String FS_FTP_PASSWORD_PREFIX = "fs.ftp.password.";
    public static final String FS_FTP_DATA_CONNECTION_MODE = "fs.ftp.data.connection.mode";
    public static final String FS_FTP_TRANSFER_MODE = "fs.ftp.transfer.mode";
    public static final String E_SAME_DIRECTORY_ONLY = "only same directory renames are supported";
    private URI uri;
    private static final String DEFAULT_ENCODING ="UTF-8";

    public CustomFtpFileSystem() {
    }

    public String getScheme() {
        return "ftp";
    }

    protected int getDefaultPort() {
        return 21;
    }

    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        String host = uri.getHost();
        host = host == null ? conf.get("fs.ftp.host", (String)null) : host;
        if (host == null) {
            throw new IOException("Invalid host specified");
        } else {
            conf.set("fs.ftp.host", host);
            int port = uri.getPort();
            port = port == -1 ? 21 : port;
            conf.setInt("fs.ftp.host.port", port);
            String userAndPassword = uri.getUserInfo();
            if (userAndPassword == null) {
                userAndPassword = conf.get("fs.ftp.user." + host, (String)null) + ":" + conf.get("fs.ftp.password." + host, (String)null);
            }

            String[] userPasswdInfo = userAndPassword.split(":");
            Preconditions.checkState(userPasswdInfo.length > 1, "Invalid username / password");
            conf.set("fs.ftp.user." + host, userPasswdInfo[0]);
            conf.set("fs.ftp.password." + host, userPasswdInfo[1]);
            this.setConf(conf);
            this.uri = uri;
        }
    }

    private FTPClient connect() throws IOException {
        FTPClient client = null;
        Configuration conf = this.getConf();
        String host = conf.get("fs.ftp.host");
        int port = conf.getInt("fs.ftp.host.port", 21);
        String user = conf.get("fs.ftp.user." + host);
        String password = conf.get("fs.ftp.password." + host);
        client = new FTPClient();
        client.setControlEncoding(DEFAULT_ENCODING);
        String controlEncoding = conf.get("ftp.client.control-encoding");
        if(StringUtils.isNotBlank(controlEncoding)){
            client.setControlEncoding(controlEncoding);
        }
        client.connect(host, port);
        int reply = client.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            throw NetUtils.wrapException(host, port, "(unknown)", 0, new ConnectException("Server response " + reply));
        } else if (client.login(user, password)) {
            client.setFileTransferMode(this.getTransferMode(conf));
            client.setFileType(2);
            client.setBufferSize(1048576);
            this.setDataConnectionMode(client, conf);
            return client;
        } else {
            throw new IOException("Login failed on server - " + host + ", port - " + port + " as user '" + user + "'");
        }
    }

    @VisibleForTesting
    int getTransferMode(Configuration conf) {
        String mode = conf.get("fs.ftp.transfer.mode");
        int ret = 11;
        if (mode == null) {
            return ret;
        } else {
            String upper = mode.toUpperCase();
            if (upper.equals("STREAM_TRANSFER_MODE")) {
                ret = 10;
            } else if (upper.equals("COMPRESSED_TRANSFER_MODE")) {
                ret = 12;
            } else if (!upper.equals("BLOCK_TRANSFER_MODE")) {
                LOG.warn("Cannot parse the value for fs.ftp.transfer.mode: " + mode + ". Using default.");
            }

            return ret;
        }
    }

    @VisibleForTesting
    void setDataConnectionMode(FTPClient client, Configuration conf) throws IOException {
        String mode = conf.get("fs.ftp.data.connection.mode");
        if (mode != null) {
            String upper = mode.toUpperCase();
            if (upper.equals("PASSIVE_LOCAL_DATA_CONNECTION_MODE")) {
                client.enterLocalPassiveMode();
            } else if (upper.equals("PASSIVE_REMOTE_DATA_CONNECTION_MODE")) {
                client.enterRemotePassiveMode();
            } else if (!upper.equals("ACTIVE_LOCAL_DATA_CONNECTION_MODE")) {
                LOG.warn("Cannot parse the value for fs.ftp.data.connection.mode: " + mode + ". Using default.");
            }

        }
    }

    private void disconnect(FTPClient client) throws IOException {
        if (client != null) {
            if (!client.isConnected()) {
                throw new FTPException("Client not connected");
            }

            boolean logoutSuccess = client.logout();
            client.disconnect();
            if (!logoutSuccess) {
                LOG.warn("Logout failed while disconnecting, error code - " + client.getReplyCode());
            }
        }

    }

    private Path makeAbsolute(Path workDir, Path path) {
        return path.isAbsolute() ? path : new Path(workDir, path);
    }

    public FSDataInputStream open(Path file, int bufferSize) throws IOException {
        FTPClient client = this.connect();
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = this.makeAbsolute(workDir, file);
        FileStatus fileStat = this.getFileStatus(client, absolute);
        if (fileStat.isDirectory()) {
            this.disconnect(client);
            throw new FileNotFoundException("Path " + file + " is a directory.");
        } else {
            client.allocate(bufferSize);
            Path parent = absolute.getParent();
            client.changeWorkingDirectory(parent.toUri().getPath());
            InputStream is = client.retrieveFileStream(file.getName());
            FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is, client, this.statistics));
            if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
                fis.close();
                throw new IOException("Unable to open file: " + file + ", Aborting");
            } else {
                return fis;
            }
        }
    }

    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        final FTPClient client = this.connect();
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = this.makeAbsolute(workDir, file);

        FileStatus status;
        try {
            status = this.getFileStatus(client, file);
        } catch (FileNotFoundException var15) {
            status = null;
        }

        if (status != null) {
            if (!overwrite || status.isDirectory()) {
                this.disconnect(client);
                throw new FileAlreadyExistsException("File already exists: " + file);
            }

            this.delete(client, file, false);
        }

        Path parent = absolute.getParent();
        if (parent != null && this.mkdirs(client, parent, FsPermission.getDirDefault())) {
            client.allocate(bufferSize);
            client.changeWorkingDirectory(parent.toUri().getPath());
            FSDataOutputStream fos = new FSDataOutputStream(client.storeFileStream(file.getName()), this.statistics) {
                public void close() throws IOException {
                    super.close();
                    if (!client.isConnected()) {
                        throw new FTPException("Client not connected");
                    } else {
                        boolean cmdCompleted = client.completePendingCommand();
                        CustomFtpFileSystem.this.disconnect(client);
                        if (!cmdCompleted) {
                            throw new FTPException("Could not complete transfer, Reply Code - " + client.getReplyCode());
                        }
                    }
                }
            };
            if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
                fos.close();
                throw new IOException("Unable to create file: " + file + ", Aborting");
            } else {
                return fos;
            }
        } else {
            parent = parent == null ? new Path("/") : parent;
            this.disconnect(client);
            throw new IOException("create(): Mkdirs failed to create: " + parent);
        }
    }

    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Append is not supported by FTPFileSystem");
    }

    private boolean exists(FTPClient client, Path file) throws IOException {
        try {
            this.getFileStatus(client, file);
            return true;
        } catch (FileNotFoundException var4) {
            return false;
        }
    }

    public boolean delete(Path file, boolean recursive) throws IOException {
        FTPClient client = this.connect();

        boolean var5;
        try {
            boolean success = this.delete(client, file, recursive);
            var5 = success;
        } finally {
            this.disconnect(client);
        }

        return var5;
    }

    private boolean delete(FTPClient client, Path file, boolean recursive) throws IOException {
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = this.makeAbsolute(workDir, file);
        String pathName = absolute.toUri().getPath();

        try {
            FileStatus fileStat = this.getFileStatus(client, absolute);
            if (fileStat.isFile()) {
                return client.deleteFile(pathName);
            }
        } catch (FileNotFoundException var12) {
            return false;
        }

        FileStatus[] dirEntries = this.listStatus(client, absolute);
        if (dirEntries != null && dirEntries.length > 0 && !recursive) {
            throw new IOException("Directory: " + file + " is not empty.");
        } else {
            FileStatus[] var8 = dirEntries;
            int var9 = dirEntries.length;

            for(int var10 = 0; var10 < var9; ++var10) {
                FileStatus dirEntry = var8[var10];
                this.delete(client, new Path(absolute, dirEntry.getPath()), recursive);
            }

            return client.removeDirectory(pathName);
        }
    }

    @VisibleForTesting
    FsAction getFsAction(int accessGroup, FTPFile ftpFile) {
        FsAction action = FsAction.NONE;
        if (ftpFile.hasPermission(accessGroup, 0)) {
            action = action.or(FsAction.READ);
        }

        if (ftpFile.hasPermission(accessGroup, 1)) {
            action = action.or(FsAction.WRITE);
        }

        if (ftpFile.hasPermission(accessGroup, 2)) {
            action = action.or(FsAction.EXECUTE);
        }

        return action;
    }

    private FsPermission getPermissions(FTPFile ftpFile) {
        FsAction user = this.getFsAction(0, ftpFile);
        FsAction group = this.getFsAction(1, ftpFile);
        FsAction others = this.getFsAction(2, ftpFile);
        return new FsPermission(user, group, others);
    }

    public URI getUri() {
        return this.uri;
    }

    public FileStatus[] listStatus(Path file) throws IOException {
        FTPClient client = this.connect();

        FileStatus[] var4;
        try {
            FileStatus[] stats = this.listStatus(client, file);
            var4 = stats;
        } finally {
            this.disconnect(client);
        }

        return var4;
    }

    private FileStatus[] listStatus(FTPClient client, Path file) throws IOException {
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = this.makeAbsolute(workDir, file);
        FileStatus fileStat = this.getFileStatus(client, absolute);
        if (fileStat.isFile()) {
            return new FileStatus[]{fileStat};
        } else {
            FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
            FileStatus[] fileStats = new FileStatus[ftpFiles.length];

            for(int i = 0; i < ftpFiles.length; ++i) {
                fileStats[i] = this.getFileStatus(ftpFiles[i], absolute);
            }

            return fileStats;
        }
    }

    public FileStatus getFileStatus(Path file) throws IOException {
        FTPClient client = this.connect();

        FileStatus var4;
        try {
            FileStatus status = this.getFileStatus(client, file);
            var4 = status;
        } finally {
            this.disconnect(client);
        }

        return var4;
    }

    private FileStatus getFileStatus(FTPClient client, Path file) throws IOException {
        FileStatus fileStat = null;
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = this.makeAbsolute(workDir, file);
        Path parentPath = absolute.getParent();
        if (parentPath == null) {
            long length = -1L;
            boolean isDir = true;
            int blockReplication = 1;
            long blockSize = 4096L;
            long modTime = -1L;
            Path root = new Path("/");
            return new FileStatus(length, isDir, blockReplication, blockSize, modTime, this.makeQualified(root));
        } else {
            String pathName = parentPath.toUri().getPath();
            FTPFile[] ftpFiles = client.listFiles(pathName);
            if (ftpFiles == null) {
                throw new FileNotFoundException("File " + file + " does not exist.");
            } else {
                FTPFile[] var9 = ftpFiles;
                int var10 = ftpFiles.length;

                for(int var11 = 0; var11 < var10; ++var11) {
                    FTPFile ftpFile = var9[var11];
                    if (ftpFile.getName().equals(file.getName())) {
                        fileStat = this.getFileStatus(ftpFile, parentPath);
                        break;
                    }
                }

                if (fileStat == null) {
                    throw new FileNotFoundException("File " + file + " does not exist.");
                } else {
                    return fileStat;
                }
            }
        }
    }

    private FileStatus getFileStatus(FTPFile ftpFile, Path parentPath) {
        long length = ftpFile.getSize();
        boolean isDir = ftpFile.isDirectory();
        int blockReplication = 1;
        long blockSize = 4096L;
        long modTime = ftpFile.getTimestamp().getTimeInMillis();
        long accessTime = 0L;
        FsPermission permission = this.getPermissions(ftpFile);
        String user = ftpFile.getUser();
        String group = ftpFile.getGroup();
        Path filePath = new Path(parentPath, ftpFile.getName());
        return new FileStatus(length, isDir, blockReplication, blockSize, modTime, accessTime, permission, user, group, this.makeQualified(filePath));
    }

    public boolean mkdirs(Path file, FsPermission permission) throws IOException {
        FTPClient client = this.connect();

        boolean var5;
        try {
            boolean success = this.mkdirs(client, file, permission);
            var5 = success;
        } finally {
            this.disconnect(client);
        }

        return var5;
    }

    private boolean mkdirs(FTPClient client, Path file, FsPermission permission) throws IOException {
        boolean created = true;
        Path workDir = new Path(client.printWorkingDirectory());
        Path absolute = this.makeAbsolute(workDir, file);
        String pathName = absolute.getName();
        if (!this.exists(client, absolute)) {
            Path parent = absolute.getParent();
            created = parent == null || this.mkdirs(client, parent, FsPermission.getDirDefault());
            if (created) {
                String parentDir = parent.toUri().getPath();
                client.changeWorkingDirectory(parentDir);
                created = created && client.makeDirectory(pathName);
            }
        } else if (this.isFile(client, absolute)) {
            throw new ParentNotDirectoryException(String.format("Can't make directory for path %s since it is a file.", absolute));
        }

        return created;
    }

    private boolean isFile(FTPClient client, Path file) {
        try {
            return this.getFileStatus(client, file).isFile();
        } catch (FileNotFoundException var4) {
            return false;
        } catch (IOException var5) {
            throw new FTPException("File check failed", var5);
        }
    }

    public boolean rename(Path src, Path dst) throws IOException {
        FTPClient client = this.connect();

        boolean var5;
        try {
            boolean success = this.rename(client, src, dst);
            var5 = success;
        } finally {
            this.disconnect(client);
        }

        return var5;
    }

    private boolean isParentOf(Path parent, Path child) {
        URI parentURI = parent.toUri();
        String parentPath = parentURI.getPath();
        if (!parentPath.endsWith("/")) {
            parentPath = parentPath + "/";
        }

        URI childURI = child.toUri();
        String childPath = childURI.getPath();
        return childPath.startsWith(parentPath);
    }

    private boolean rename(FTPClient client, Path src, Path dst) throws IOException {
        Path workDir = new Path(client.printWorkingDirectory());
        Path absoluteSrc = this.makeAbsolute(workDir, src);
        Path absoluteDst = this.makeAbsolute(workDir, dst);
        if (!this.exists(client, absoluteSrc)) {
            throw new FileNotFoundException("Source path " + src + " does not exist");
        } else {
            if (this.isDirectory(absoluteDst)) {
                absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
            }

            if (this.exists(client, absoluteDst)) {
                throw new FileAlreadyExistsException("Destination path " + dst + " already exists");
            } else {
                String parentSrc = absoluteSrc.getParent().toUri().toString();
                String parentDst = absoluteDst.getParent().toUri().toString();
                if (this.isParentOf(absoluteSrc, absoluteDst)) {
                    throw new IOException("Cannot rename " + absoluteSrc + " under itself : " + absoluteDst);
                } else if (!parentSrc.equals(parentDst)) {
                    throw new IOException("Cannot rename source: " + absoluteSrc + " to " + absoluteDst + " -" + "only same directory renames are supported");
                } else {
                    String from = absoluteSrc.getName();
                    String to = absoluteDst.getName();
                    client.changeWorkingDirectory(parentSrc);
                    boolean renamed = client.rename(from, to);
                    return renamed;
                }
            }
        }
    }

    public Path getWorkingDirectory() {
        return this.getHomeDirectory();
    }

    public Path getHomeDirectory() {
        FTPClient client = null;

        Path var3;
        try {
            client = this.connect();
            Path homeDir = new Path(client.printWorkingDirectory());
            var3 = homeDir;
        } catch (IOException var12) {
            throw new FTPException("Failed to get home directory", var12);
        } finally {
            try {
                this.disconnect(client);
            } catch (IOException var11) {
                throw new FTPException("Failed to disconnect", var11);
            }
        }

        return var3;
    }

    public void setWorkingDirectory(Path newDir) {
    }
}
