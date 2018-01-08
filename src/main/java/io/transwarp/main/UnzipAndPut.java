package io.transwarp.main;

import io.transwarp.util.CommandLineValues;
import io.transwarp.util.DFSClient;
import io.transwarp.util.FileOperator;
import io.transwarp.util.MyProcess;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

/**
 * 解压、上传源数据
 * java -classpath E:\IdeaProjects\ltjt_process\out\artifacts\testUnzipAndPut_jar\ltjt_process.jar io.transwarp.main.UnzipAndPut
 */
public class UnzipAndPut {
    private static String clazz = UnzipAndPut.class.getName();
    private static Logger logger = Logger.getLogger(clazz);

    private DFSClient dfsClient = new DFSClient();

    public UnzipAndPut(String prop) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(prop);
            Properties properties = new Properties();
            properties.load(fis);
            dfsClient.setDfsUri(properties.getProperty("uri", "hdfs:///"));
            dfsClient.setHdfsUser(properties.getProperty("user", "hdfs"));
            dfsClient.setAuthentication(properties.getProperty("auth", "simple"));
            dfsClient.login();
//      dfsClient.setHdfsGroup(properties.getProperty("group", ""));
            dfsClient.setFsPermission(properties.getProperty("permission", "755"));
//      dfsClient.setFsStat(dfsClient.getHdfs().getStatus());
            logger.info("Loading " + this.getClass().getName() + " constructor successfully");
        } catch (Exception e) {
            logger.error("Error occurs when loading " + this.getClass().getName() + " constructor", e);
        }
    }

    public static void main(String[] args) {
        try {
            CommandLineValues cm = new CommandLineValues(args);
            cm.parseCmd(UnzipAndPut.class.getName());
            String dstDir = cm.getDstDir();
            MyProcess myProcess = new MyProcess();
//      List<String> srcList = myProcess.grepPath(false, cm.getSrcDir(), cm.getCdrKey());
//      logger.info(srcList.get(0));
            List<File> srcList = myProcess.grepFile(false, cm.getSrcDir(), cm.getCdrKey());
            logger.info(srcList.get(0).getAbsolutePath());
            FileOperator fileOperator = new FileOperator();
            UnzipAndPut myself = new UnzipAndPut(cm.getConf());
            for (File src : srcList) {
                String entryPath = fileOperator.unTarGz(src.getAbsolutePath(), cm.getUnzipDir());
                fileOperator.gzip(entryPath);
                myself.dfsClient.makeDir(dstDir, myself.dfsClient.getFsPermission());
                myself.dfsClient.putOne(cm.isDelTmp(), cm.isOverwrite(), entryPath, dstDir);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
