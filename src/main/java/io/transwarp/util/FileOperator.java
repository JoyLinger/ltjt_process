package io.transwarp.util;

import org.apache.log4j.Logger;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 文件压缩与解压类
 */
public class FileOperator {
    private Logger logger = Logger.getLogger(this.getClass());

    public FileOperator() {
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public static void main(String[] args) {
        String str = "/home/guoqiang.ma/data.log";
        System.out.println(str);
        long start = System.currentTimeMillis();
        FileOperator fo = new FileOperator();
        fo.gzip(str);
        long end = System.currentTimeMillis();
        System.out.println("spent " + (end - start) + " ms");
    }

    public boolean createDir(String destDirName) {
        File dir = new File(destDirName);
        if (dir.exists()) {
            logger.info("目录" + destDirName + "已存在");
            return false;
        }
        if (!destDirName.endsWith(File.separator)) {
            destDirName = destDirName + File.separator;
        }
        //创建目录
        if (dir.mkdirs()) {
            logger.info("创建目录" + destDirName + "成功！");
            return true;
        } else {
            logger.info("创建目录" + destDirName + "失败！");
            return false;
        }
    }

    /**
     * 解压tar.gz 文件
     *
     * @param path      要解压的tar.gz文件路径
     * @param outputDir 要解压到某个指定的目录下
     */
    public String unTarGz(String path, String outputDir) {
        String tmpFile = null;
        TarInputStream tarIn = null;
        try {
            tarIn = new TarInputStream(new GZIPInputStream(
                    new BufferedInputStream(new FileInputStream(path))), 1024 * 2);
            createDir(outputDir);//创建输出目录
            TarEntry entry;
            while ((entry = tarIn.getNextEntry()) != null) {
                if (entry.isDirectory()) {//是目录
                    entry.getName();
                    createDir(outputDir + entry.getName());//创建空目录
                } else {//是文件
                    tmpFile = outputDir + "/" + entry.getName();
                    OutputStream out = null;
                    try {
                        out = new FileOutputStream(tmpFile);
                        int length;
                        byte[] b = new byte[2048];
                        while ((length = tarIn.read(b)) != -1) {
                            out.write(b, 0, length);
                        }
                    } catch (IOException ex) {
                        logger.error(ex.getMessage(), ex);
                    } finally {
                        if (out != null)
                            out.close();
                    }
                }
            }
        } catch (IOException ex) {
            logger.error("解压归档文件出现异常", ex);
        } finally {
            try {
                if (tarIn != null) {
                    tarIn.close();
                }
            } catch (IOException ex) {
                logger.error("关闭tarFile出现异常", ex);
            }
        }
        return tmpFile;
    }

    /**
     * 将文件压缩为*.gz格式
     *
     * @param path 需要压缩的文件路径
     */
    public void gzip(String path) {
        String outFileName = path + ".gz";
        FileInputStream in = null;
        try {
            in = new FileInputStream(new File(path));
        } catch (FileNotFoundException e) {
            logger.error("Could not find the inFile..." + path, e);
        }
        GZIPOutputStream out = null;
        try {
            out = new GZIPOutputStream(new FileOutputStream(outFileName));
        } catch (IOException e) {
            logger.error("Could not find the outFile..." + outFileName, e);
        }
        byte[] buf = new byte[10240];
        int len;
        try {
            assert in != null;
            while (((in.available() > 10240) && (in.read(buf)) > 0)) {
                assert out != null;
                out.write(buf);
            }
            len = in.available();
            in.read(buf, 0, len);
            assert out != null;
            out.write(buf, 0, len);
            in.close();
            out.flush();
            out.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
