package io.transwarp.main;

import io.transwarp.core.Create;
import io.transwarp.util.CommandLineValues;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

public class GenerateAndRunSql {
    private static Logger logger = Logger.getLogger(GenerateAndRunSql.class);
    private Create creater = new Create();

    private GenerateAndRunSql(String prop) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(prop);
            Properties properties = new Properties();
            properties.load(fis);
            creater.setSqlConfigDelimiter(properties.getProperty("SQL_CONFIG_DELIMITER"));
            creater.setCreateDatabaseSyntax(properties.getProperty("CREATE_DATABASE_SYNTAX"));
            creater.setCreateTableSyntax(properties.getProperty("CREATE_TABLE_SYNTAX"));
            logger.info("Loading " + this.getClass().getName() + " constructor successfully");
        } catch (Exception e) {
            logger.error("Error occurs when loading " + this.getClass().getName() + " constructor", e);
        }
    }

    public static void main(String[] args) {
        try {
            CommandLineValues cm = new CommandLineValues(args);
            cm.parseCmd(UnzipAndPut.class.getName());
            GenerateAndRunSql myself = new GenerateAndRunSql(cm.getConf());
            myself.creater.GenerateSqlFromFile(cm.getSqlConfigFile());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
