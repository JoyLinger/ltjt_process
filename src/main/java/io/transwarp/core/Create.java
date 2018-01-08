package io.transwarp.core;

import io.transwarp.constant.SQL_TYPE;
import io.transwarp.util.Read;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Create {
    private static Create creater = new Create();
    // 数据库信息
    private final String EXT_DB = "sc_ext";
    private final String BUFFER_DB = "sc_buffer";
    private final String FINAL_DB = "sc_final";
    //  private static final String OFFLINE_DB = "sc_offline";
//  private static final String RUNTIME_DB = "sc_runtime";
    // 表信息
    private final String EXT_SUFFIX = "_ext";
    private final String BUFFER_SUFFIX = "_buffer";
    private final String FINAL_SUFFIX = "_final";
    // 外表信息
    private final String EXT_TYPE = "EXTERNAL";
    private final String EXT_HDFS_PATH = "/user/xh/external/user";
    private final String row_format = "DELIMITED";
    private final String column_delimiter = "\\001";
    private final String complex_type_delimiter = "\\002";
    private final String kv_delimiter = "\\003";
    private final String newline_char = "\\n";
    // 分区信息
    private final String PARTITION_KEY = "begin_time";
    private final String PARTITION_COMMENT = PARTITION_KEY + "为分区建";
    private final String PARTITION_FILE = "src/table_fields/partitions";
    // 分桶信息
    private final String CLUSTER_KEY = "imsi";
    private final String CLUSTER_COMMENT = CLUSTER_KEY + "为分桶建";
    private final int NUM_BUCKETS = 191;
    // 文件格式
    private final String FILE_FORMAT = "ORC";
    // 换行符
    private final String newLine = "\n";
    // 切换数据库SQL
    private final String USE_SYNTAX = "USE <db_type> <db_name>;";
    private Logger logger = Logger.getLogger(this.getClass());
    private Read reader = new Read();
    //  private static final String OFFLINE_SUFFIX = "_ext";
//  private static final String RUNTIME_SUFFIX = "_ext";
    // 表注释
    private Map<String, String> tableComment = new HashMap<String, String>();
    private String columnDelimiter = null;
    private String newlineChar = null;


    // 创建数据库语法
    private String createDatabaseSyntax = null;
    // 建表语法
    private String createTableSyntax = null;
    // SQL配置文件分隔符
    private String sqlConfigDelimiter = null;

    public Create() {
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public static void main(String[] args) {
//      creater.ltSqlGen(null, null);
        String sqls = creater.GenerateSqlFromFile("src/table_fields/user_general_sql.config");
        System.out.println(sqls);
    }

    public void setCreateDatabaseSyntax(String createDatabaseSyntax) {
        this.createDatabaseSyntax = createDatabaseSyntax;
    }

    public void setCreateTableSyntax(String createTableSyntax) {
        this.createTableSyntax = createTableSyntax;
    }

    public void setSqlConfigDelimiter(String sqlConfigDelimiter) {
        this.sqlConfigDelimiter = sqlConfigDelimiter;
    }

    /**
     * @deprecated
     */
    private String createNormalTable(String sql, String columns, String tableComment) {
        return sql.replace("<cols_name_type_comment>", columns)
                .replace("<table_comment>", tableComment);
    }

    /**
     * @deprecated
     */
    private String createETTable(String sql, String tableType, String tableName, String rowFormat, String columnDelimiter, String complexTypeDelimiter, String kvDelimiter, String newlineChar, String hdfsPath) {
        return sql.replace("<table_type>", tableType == null ? this.EXT_TYPE : tableType)
                .replace("<table_name>", tableName)
                .replace("<row_format>", rowFormat)
                .replace("<column_delimiter>", columnDelimiter)
                .replace("<complex_type_delimiter>", complexTypeDelimiter)
                .replace("<kv_delimiter>", kvDelimiter)
                .replace("<newLine>", newlineChar)
                .replace("<hdfs_path>", hdfsPath);
    }

    /**
     * @deprecated
     */
    private String createExtTable(String sql, String tableName, String columnDelimiter
            , String newlineChar, String hdfsPath) {
        String useDB = this.USE_SYNTAX.replace("<db_type> <db_name>", this.EXT_DB);
        sql = createETTable(sql, this.EXT_TYPE, tableName, this.row_format, columnDelimiter
                , this.complex_type_delimiter, this.kv_delimiter, newlineChar, hdfsPath);
        return useDB + "\n" + formatSql(sql);
    }

    /**
     * @deprecated
     */
    private String createBufferTable(String sql, String tableName, String partitions) {
        String useDB = this.USE_SYNTAX.replace("<db_type> <db_name>", this.BUFFER_DB);
        sql = sql.replace("<table_type>", "")
                .replace("<table_name>", tableName)
                .replace("<part_key>", this.PARTITION_KEY)
                .replace("<partition_comment>", this.PARTITION_COMMENT)
                .replace("<partitions>", partitions)
                .replace("<cluster_key>", this.CLUSTER_KEY)
                .replace("<cluster_comment>", this.CLUSTER_COMMENT)
                .replace("<num_buckets>", this.NUM_BUCKETS + "")
                .replace("<file_format>", this.FILE_FORMAT);
        return useDB + "\n" + formatSql(sql);
    }

    /**
     * @deprecated
     */
    private String createFinalTable(String sql, String tableName, String partitions) {
        String useDB = this.USE_SYNTAX.replace("<db_type> <db_name>", this.FINAL_DB);
        sql = sql.replace("<table_type>", "")
                .replace("<table_name>", tableName)
                .replace("<part_key>", this.PARTITION_KEY)
                .replace("<partition_comment>", this.PARTITION_COMMENT)
                .replace("<partitions>", partitions)
                .replace("<cluster_key>", this.CLUSTER_KEY)
                .replace("<cluster_comment>", this.CLUSTER_COMMENT)
                .replace("<num_buckets>", this.NUM_BUCKETS + "")
                .replace("<file_format>", this.FILE_FORMAT);
        return useDB + "\n" + formatSql(sql);
    }

    /**
     * @deprecated
     */
    public void ltSqlGen(String columnDelimiter, String newlineChar) {
        columnDelimiter = columnDelimiter == null ? this.columnDelimiter : columnDelimiter;
        newlineChar = newlineChar == null ? this.newlineChar : newlineChar;
        for (String cdr : this.tableComment.keySet()) {
            // 外表
            String tableName = cdr + this.EXT_SUFFIX;
            String hdfsPath = this.EXT_HDFS_PATH + tableName;
            String cols = reader.readAsString("src/table_fields/" + tableName + ".cols");
            String sql = creater.createNormalTable(this.createTableSyntax, cols, this.tableComment.get(cdr));
            String extSql = creater.createExtTable(sql, tableName, columnDelimiter, newlineChar, hdfsPath);
            System.out.println(extSql);
            // buffer表
            tableName = cdr + this.BUFFER_SUFFIX;
            String partitions = reader.readAsString(this.PARTITION_FILE);
            String bufferSql = creater.createBufferTable(sql, tableName, partitions);
            System.out.println(bufferSql);
            // final表
            tableName = cdr + this.FINAL_SUFFIX;
            String finalCols = reader.readAsString("src/table_fields/" + tableName + ".cols");
            String finalSql = creater.createNormalTable(this.createTableSyntax, finalCols, this.tableComment.get(cdr));
            finalSql = creater.createFinalTable(finalSql, tableName, partitions);
            System.out.println(finalSql);
        }
    }

    private String formatSql(String sql) {
        return sql.replaceAll(".*<.*>.*" + newLine, "");
    }

    public String createDatabase(String databaseName) {
        return createDatabaseSyntax.replace("<db_name>", databaseName);
    }

    public String createTable(String[] args) {
        String partitions = args[7].trim().equals("") ? "" : reader.readAsString(args[7]);
        String property_pairs = args[19].trim().equals("") ? "" : reader.readAsString(args[19]);
        return createTableSyntax.replace("<table_type>", args[1])
                .replace("<table_name>", args[2])
                .replace("<cols_name_type_comment>", reader.readAsString(args[3]))
                .replace("<table_comment>", args[4])
                .replace("<part_key>", args[5].equals("") ? "<>" : args[5])
                .replace("<partition_comment>", args[6].equals("") ? "<>" : args[6])
                .replace("<partitions>", (partitions == null || partitions.equals("")) ? "<>" : partitions)
                .replace("<cluster_key>", args[8].equals("") ? "<>" : args[8])
                .replace("<cluster_comment>", args[9].equals("") ? "<>" : args[9])
                .replace("<num_buckets>", args[10].equals("") ? "<>" : args[10])
                .replace("<row_format>", args[11].equals("") ? "<>" : args[11])
                .replace("<column_delimiter>", args[12].equals("") ? "<>" : args[12])
                .replace("<complex_type_delimiter>", args[13].equals("") ? "<>" : args[13])
                .replace("<kv_delimiter>", args[14].equals("") ? "<>" : args[14])
                .replace("<newLine>", args[15].equals("") ? "<>" : args[15])
                .replace("<file_format>", args[16].equals("") ? "<>" : args[16])
                .replace("<storage.handler.class.name>", args[17].equals("") ? "<>" : args[17])
                .replace("<hdfs_path>", args[18].equals("") ? "<>" : args[18])
                .replace("<property_pairs>", (property_pairs == null || property_pairs.equals("")) ? "<>" : property_pairs);
    }

    public String GenerateSqlFromFile(String path) {
        StringBuilder sqls = new StringBuilder();
        try {
            List<String> lines = reader.readLinesAsList(path);
            for (String line : lines) {
                sqls.append("\n");
                String[] sqlConfigs = line.split(sqlConfigDelimiter, -1);
                switch (SQL_TYPE.parseSqlType(sqlConfigs[0])) {
                    case CREATE_DATABASE:
                        sqls.append(createDatabase(sqlConfigs[1]));
                        break;
                    case CREATE_TABLE:
                        sqls.append(createTable(sqlConfigs));
                        break;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.error(e, e);
        }
        return formatSql(sqls.toString());
    }
}
