package io.transwarp.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 处理命令行参数
 */
public class CommandLineValues {
    private static final String DEFAULT_CONF = "target/classes/unicom.properties";
    private Logger logger = Logger.getLogger(this.getClass());
    private CmdLineParser parser;
    private String[] arguments;

    // Virtual Machine parameter
    @Option(name = "-D", metaVar = "<property>=<value>", usage = "Use value for given property(property=value)")
    private Map<String, String> prop = new HashMap<String, String>();

    // Program parameter
    // 0. property file
    @Option(name = "-conf", metaVar = "<configuration file>", usage = "Specify an application configuration file", forbids = {"-P", "-S"})
    private String conf = DEFAULT_CONF;

    // 1. put mode
    @Option(name = "-P", aliases = "--put-mode", usage = "Execute put mode", forbids = {"-S", "--sql-mode"})
    private boolean put = false;

    @Option(name = "-sd", aliases = "--source-dir", metaVar = "[put|sql]", usage = "Parent directory of data sources", depends = "-P", forbids = {"-conf"})
    private String srcDir = "";

    @Option(name = "-dst", aliases = "--destination-dir", usage = "Destination hdfs directory to put into", depends = {"-P"}, forbids = {"-conf"})
    private String dstDir = "";

    @Option(name = "-u", aliases = "--unzip-dir", usage = "Directory to unzip into", depends = {"-P"}, forbids = {"-conf"})
    private String unzipDir = "";

    @Option(name = "-r", aliases = "--regex-pattern", usage = "Regex pattern to grep", depends = {"-P"}, forbids = {"-conf"})
    private String cdrKey = "";

    @Option(name = "-del", aliases = "--del-tmp", usage = "If delete after putting the file to hdfs", depends = {"-P"}, forbids = {"-conf"})
    private boolean delTmp = true;

    @Option(name = "-o", aliases = "--if-overwrite", usage = "If overwrite the existing hdfs file when putting", depends = {"-P"}, forbids = {"-conf"})
    private boolean overwrite = true;

    // 2. SQL_TYPE mode
    @Option(name = "-S", aliases = "--sql-mode", usage = "Execute sql mode", forbids = {"-P", "--put-mode"})
    private boolean sql = false;

    @Option(name = "-sc", aliases = "--sql-config", usage = "Sql config file", depends = {"-S"}, forbids = {"-conf"})
    private String sqlConfigFile;

    @Option(name = "-v", aliases = "--version", usage = "Print file putter version")
    private boolean isVersion;

    @Option(name = "-h", aliases = "--help", usage = "Print help information")
    private boolean isHelp;


    public CommandLineValues(String... args) {
        parser = new CmdLineParser(this);
        arguments = args;
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public void initConf() {
        FileInputStream fis;
        try {
            fis = new FileInputStream(conf);
            Properties properties = new Properties();
            properties.load(fis);
            put = properties.getProperty("put", "false").equals("true");
            srcDir = properties.getProperty("srcDir", ".");
            dstDir = properties.getProperty("dstDir", ".");
            unzipDir = properties.getProperty("unzipDir", "/tmp");
            cdrKey = properties.getProperty("cdrKey", "");
            delTmp = properties.getProperty("delTmp", "true").equals("true");
            overwrite = properties.getProperty("overwrite", "true").equals("true");
            sql = properties.getProperty("sql", "true").equals("true");
            sqlConfigFile = properties.getProperty("sqlConfig", "*.config");
            isHelp = properties.getProperty("isHelp", "false").equals("true");
            isVersion = properties.getProperty("isVersion", "false").equals("true");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void parseCmd(String mainClass) {

        try {
            if (ArrayUtils.contains(arguments, "-conf")) {
                parser.parseArgument(arguments);
                initConf();
            } else {
                initConf();
                parser.parseArgument(arguments);
            }
            //Pull or Push mode must be specified.
            if (!isHelp() && !isVersion() && !isPut() && !isSql()) {
                throw new CmdLineException(parser, "Error: One of the Put or Sql mode must be selected.", new Throwable());
            }
            //ZK addresses must be set
            if (!isHelp() && !isVersion() && getUnzipDir().equals("/tmp")) {
                logger.warn("Default unzip directory(/tmp) is used");
            }
            // If help is needed
            if (isHelp()) {
                throw new CmdLineException(parser, "", new Throwable());
            }
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
//      System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright @CMBC, Author : huangpengcheng@cmbc.com.cn");
            System.err.println("Usage: " + mainClass + " [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
            System.exit(-1);
        }
//    System.out.println("Hadoop File Updater, Version:" + CONSTANTSUTIL.VERSION + ", Copyright @CMBC, Author : huangpengcheng@cmbc.com.cn");
    }

    public String getSrcDir() {
        return srcDir;
    }

    public String getDstDir() {
        return dstDir;
    }

    public String getCdrKey() {
        return cdrKey;
    }

    public String getUnzipDir() {
        return unzipDir;
    }

    public boolean isDelTmp() {
        return delTmp;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public boolean isPut() {
        return put;
    }

    public boolean isSql() {
        return sql;
    }

    public boolean isVersion() {
        return isVersion;
    }

    public boolean isHelp() {
        return isHelp;
    }

    public String getConf() {
        return conf;
    }

    public Map<String, String> getProp() {
        return prop;
    }

    public String getSqlConfigFile() {
        return sqlConfigFile;
    }

    public void setSqlConfigFile(String sqlConfigFile) {
        this.sqlConfigFile = sqlConfigFile;
    }
}
