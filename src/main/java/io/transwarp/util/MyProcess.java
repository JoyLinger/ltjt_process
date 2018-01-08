package io.transwarp.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Command executor
 */
public class MyProcess {
    private Logger logger = Logger.getLogger(this.getClass());

    public MyProcess() {
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public static void main(String[] args) {
        String str = "1|2|3|";
        System.out.println(str.split("|").length);
        System.out.println(str.split("\\|", -1).length);
    }

    public String[] execAndRetrunArray(String command) {
        return (String[]) exec(command).toArray();
    }

    public List<String> exec(String command) {
        List<String> results = new ArrayList<String>();
        try {
//      String[] cmds = {"/bin/sh", "-c", command};
            String[] cmds = {command};
            Process process = Runtime.getRuntime().exec(cmds);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                results.add(line);
            }
//      process.waitFor();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return results;
    }

    /**
     * 查找指定目录下"包含"指定关键字的文件
     *
     * @param recursive 支持递归
     * @param dirPath   查找目录
     * @param regex     文件关键字
     * @return 集合对象的方式返回所有符合文件的绝对路径
     */
    public List<String> grepPath(boolean recursive, String dirPath, final String regex) {
        ArrayList<String> fileList = new ArrayList<String>();
        // dir: 递归
        FilenameFilter df = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir + "/" + name).isDirectory();
            }
        };
        // regex：关键字
        FilenameFilter ff = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(regex);
            }
        };
        // 无论是否递归，先找出dirPath目录下的文件
        File rootDir = new File(dirPath);
        String[] files = rootDir.list(ff);
        assert files != null;
        for (String f : files) {
            fileList.add(dirPath + "/" + f);
        }
        // 若递归，则继续找子目录下的文件
        if (recursive) {
            String[] dirs = rootDir.list(df);
            assert dirs != null;
            for (String d : dirs) {
                fileList.addAll(grepPath(true, dirPath + "/" + d, regex));
            }
        }
        return fileList;
    }

    /**
     * 查找指定目录下"包含"指定关键字的文件对象
     *
     * @param recursive 支持递归
     * @param dirPath   查找目录
     * @param regex     文件关键字
     * @return 集合对象的方式返回所有符合文件对象
     */
    public List<File> grepFile(boolean recursive, String dirPath, final String regex) {
        ArrayList<File> fileList = new ArrayList<File>();
        // dir: 递归
        FilenameFilter df = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir + "/" + name).isDirectory();
            }
        };
        // regex：关键字
        FilenameFilter ff = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains(regex);
            }
        };
        // 无论是否递归，先找出dirPath目录下的文件
        File rootDir = new File(dirPath);
        File[] files = rootDir.listFiles(ff);
        assert files != null;
        fileList.addAll(Arrays.asList(files));
        // 若递归，则继续找子目录下的文件
        if (recursive) {
            File[] dirs = rootDir.listFiles(df);
            assert dirs != null;
            for (File d : dirs) {
                fileList.addAll(grepFile(true, d.getAbsolutePath(), regex));
            }
        }
        return fileList;
    }
}
