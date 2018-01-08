package io.transwarp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

public class DFSClient {
    private Logger logger = Logger.getLogger(this.getClass());
    private Configuration conf = new Configuration();
    private String dfsUri = null;
    private FileSystem hdfs = null;
    private FsStatus fsStat = null;
    private String hdfsUser = "";
    private String hdfsGroup = "";
    private String authentication = "";
    private String keytab = "";
    private FsPermission fsPermission = null;

    public DFSClient() {
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public void login() throws IOException, InterruptedException {
        if ("SIMPLE".equalsIgnoreCase(authentication)) {
            this.setHdfs(FileSystem.get(URI.create(dfsUri), conf, hdfsUser));
        } else if ("LDAP".equalsIgnoreCase(authentication)) {
            this.setHdfs(FileSystem.get(URI.create(dfsUri), conf, hdfsUser));
        } else if ("KERBEROS".equalsIgnoreCase(authentication)) {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(hdfsUser, keytab);
            this.setHdfs(FileSystem.get(URI.create(dfsUri), conf));
        } else {
            logger.error("Invalid authentication: " + authentication);
//        throw new InvalidInputException("Invalid authentication: " + authentication);
        }
    }

    public long getDFSDirSize(String dst, int type) {
        long size = 0L;
        try {
            if (dst != null) {
                Path filenamePath = new Path(dst);
                if (hdfs.exists(filenamePath)) {
                    switch (type) {
                        case -1:
                            // 会根据集群的配置输出，例如我这里输出3G
                            size = hdfs.getContentSummary(filenamePath).getSpaceConsumed();
                            break;
                        default:
                            // 显示实际的输出，例如这里显示 1G
                            size = hdfs.getContentSummary(filenamePath).getLength();
                            break;
                    }
                }
            }
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
        }
        return size;
    }

    public long getDFSCapacity() {
        return fsStat.getCapacity();
    }

    public long getDFSUsed() {
        return fsStat.getUsed();
    }

    public long getDFSRemaining() {
        return fsStat.getRemaining();
    }

    public void makeDir(String path) {
        try {
            hdfs.mkdirs(new Path(path));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void makeDir(String path, FsPermission fsPermission) {
        try {
            hdfs.mkdirs(new Path(path), fsPermission);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void putOne(boolean delSrc, boolean overwrite, String from, String to) {
        try {
            if (hdfs.exists(new Path(to))) {
                hdfs.copyFromLocalFile(delSrc, overwrite, new Path(from), new Path(to));
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void putArray(boolean delSrc, boolean overwrite, String[] srcs, String dst) {
        try {
            Path[] srcPaths = new Path[srcs.length];
            for (int i = 0; i < srcs.length; i++) {
                String src = srcs[i];
                srcPaths[i] = new Path(src);
            }
            if (hdfs.exists(new Path(dst))) {
                hdfs.copyFromLocalFile(delSrc, overwrite, srcPaths, new Path(dst));
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public String getDfsUri() {
        return dfsUri;
    }

    public void setDfsUri(String dfsUri) {
        this.dfsUri = dfsUri;
    }

    public FileSystem getHdfs() {
        return hdfs;
    }

    public void setHdfs(FileSystem hdfs) {
        this.hdfs = hdfs;
    }

    public FsStatus getFsStat() {
        return fsStat;
    }

    public void setFsStat(FsStatus fsStat) {
        this.fsStat = fsStat;
    }

    public String getHdfsUser() {
        return hdfsUser;
    }

    public void setHdfsUser(String hdfsUser) {
        this.hdfsUser = hdfsUser;
    }

    public String getHdfsGroup() {
        return hdfsGroup;
    }

    public void setHdfsGroup(String hdfsGroup) {
        this.hdfsGroup = hdfsGroup;
    }

    public FsPermission getFsPermission() {
        return fsPermission;
    }

    public void setFsPermission(String numberFormat) {
        this.fsPermission = new FsPermission(numberFormat);
    }

    public void setFsPermission(FsPermission fsPermission) {
        this.fsPermission = fsPermission;
    }

    /**
     * 设置文件权限
     *
     * @param path hdfs文件路径
     * @TODO complete this method
     */
    public void setFilePermission(String path) throws IOException {
        hdfs.setPermission(new Path(path), this.fsPermission);
    }

    /**
     * 设置文件权限
     *
     * @param path         hdfs文件路径
     * @param numberFormat 数字格式的权限串, e.g. 755
     * @TODO complete this method
     */
    public void setFilePermission(String path, String numberFormat) throws IOException {
        hdfs.setPermission(new Path(path), new FsPermission(numberFormat));
    }

    /**
     * 设置文件所有者
     *
     * @param path hdfs文件路径
     */
    public void setFileOwner(String path) throws IOException {
        hdfs.setOwner(new Path(path), this.hdfsUser, this.hdfsGroup);
    }

    /**
     * 设置文件所有者
     *
     * @param path  hdfs文件路径
     * @param user  用户
     * @param group 属组
     */
    public void setFileOwner(String path, String user, String group) throws IOException {
        hdfs.setOwner(new Path(path), user, group);
    }

}
