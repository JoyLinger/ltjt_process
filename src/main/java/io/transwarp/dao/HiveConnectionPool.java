package io.transwarp.dao;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

/**
 * //一个效果非常不错的JAVA数据库连接池.
 * //from:http://www.jxer.com/home/?uid-195-action-viewspace-itemid-332
 * //虽然现在用APACHE COMMONS DBCP可以非常方便的建立数据库连接池，
 * //但是像这篇文章把数据库连接池的内部原理写的这么透彻，注视这么完整， //真是非常难得，让开发人员可以更深层次的理解数据库连接池，真是非常感
 * //谢这篇文章的作者。
 */
public class HiveConnectionPool {
    private static Logger logger = Logger.getLogger(HiveConnectionPool.class);
    private String authentication;
    // server info
    private String SERVER_IP;
    private String PORT;
    private String DB_NAME;
    // LDAP user info
    private String LDAP_USER;
    private String LDAP_PASSWD;
    // Kerberos info
    private String HIVE_PRINCIPAL;
    private String USER_PRINCIPAL;
    private String REALM;
    private String KRB5CONF;
    private String KEYTAB;
    private String DRIVER_CLASS;
    // 测试连接是否可用的测试表名，默认没有测试表
    private String testTable;
    // 连接池的初始大小
    private int initialConnections;
    // 连接池自动增加的大小
    private int incrementalConnections;
    // 连接池最大的大小
    private int maxConnections;
    /**
     * 存放连接池中数据库连接的向量 ,初始时为 null 它中存放的对象为 PooledConnection 型
     */
    private Vector<PooledConnection> connections = null;

    public HiveConnectionPool() {
        Properties properties = new Properties();
        FileInputStream fis;
        try {
            fis = new FileInputStream("connection.properties");
            properties.load(fis);
            authentication = properties.getProperty("authentication");
            SERVER_IP = properties.getProperty("SERVER_IP");
            PORT = properties.getProperty("PORT", "10000");
            DB_NAME = properties.getProperty("DB_NAME", "default");
            LDAP_USER = properties.getProperty("LDAP_USER");
            LDAP_PASSWD = properties.getProperty("LDAP_PASSWD");
            HIVE_PRINCIPAL = properties.getProperty("HIVE_PRINCIPAL");
            USER_PRINCIPAL = properties.getProperty("USER_PRINCIPAL");
            REALM = properties.getProperty("REALM");
            KRB5CONF = properties.getProperty("KRB5CONF");
            KEYTAB = properties.getProperty("KEYTAB");
            DRIVER_CLASS = properties.getProperty("DRIVER_CLASS", "org.apache.hive.jdbc.HiveDriver");
            testTable = properties.getProperty("testTable");
            initialConnections = Integer.parseInt(properties.getProperty("initialConnections"));
            incrementalConnections = Integer.parseInt(properties.getProperty("incrementalConnections"));
            maxConnections = Integer.parseInt(properties.getProperty("maxConnections"));
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * KERBEROS
     *
     * @return
     * @throws Exception
     */
    private Connection getKerberosConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:hive2://" + SERVER_IP + ":" + PORT + "/"
                        + DB_NAME + ";" + "principal=" + HIVE_PRINCIPAL + ";"
                        + "authentication=" + authentication.toLowerCase()
                        + ";" + "kuser=" + USER_PRINCIPAL + "@" + REALM + ";"
                        + "krb5conf=" + KRB5CONF + ";" + "keytab=" + KEYTAB,
                "", "");
    }

    /**
     * LDAP
     *
     * @return
     * @throws SQLException
     */
    private Connection getLDAPConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hive2://" + SERVER_IP + ":"
                + PORT + "/" + DB_NAME, LDAP_USER, LDAP_PASSWD);
        // return
        // DriverManager.getConnection("jdbc:hive2://172.16.0.44:10000/default",
        // "user", "passwd");
    }

    /**
     * SIMPLE
     *
     * @return
     * @throws SQLException
     */
    private Connection getSimpleConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hive2://" + SERVER_IP + ":"
                + PORT + "/" + DB_NAME);
    }

    // public HiveConnectionPool(Map<String,String> DBinfo){
    // this.driver = DBinfo.get("driver");
    // this.dbUrl = DBinfo.get("dbUrl");
    // this.dbUsername = DBinfo.get("dbUsername");
    // this.dbPassword = DBinfo.get("dbPassword");
    // }
    // public HiveConnectionPool(String driver,String dbUrl,String
    // dbUsername,String dbPassword)
    // {
    // this.driver = driver;
    // this.dbUrl = dbUrl;
    // this.dbUsername = dbUsername;
    // this.dbPassword = dbPassword;
    // }
    public int getInitialConnections() {
        return this.initialConnections;
    }

    public void setInitialConnections(int initialConnections) {
        this.initialConnections = initialConnections;
    }

    public int getIncrementalConnections() {
        return this.incrementalConnections;
    }

    public void setIncrementalConnections(int incrementalConnections) {
        this.incrementalConnections = incrementalConnections;
    }

    public int getMaxConnections() {
        return this.maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public String getTestTable() {
        return this.testTable;
    }

    public void setTestTable(String testTable) {
        this.testTable = testTable;
    }

    public synchronized void createPool() throws Exception {
        // 确保连接池没有创建
        // 假如连接池己经创建了，保存连接的向量 connections 不会为空
        if (connections != null) {
            return; // 假如己经创建，则返回
        }
        // 实例化 Driver 中指定的驱动类实例
        Driver driver = (Driver) (Class.forName(this.DRIVER_CLASS)
                .newInstance());
        DriverManager.registerDriver(driver); // 注册 JDBC 驱动程序
        // 创建保存连接的向量 , 初始时有 0 个元素
        connections = new Vector<PooledConnection>();
        // 根据 initialConnections 中设置的值，创建连接。
        createConnections(this.initialConnections);
        logger.info("Database connection pool creation successed!");
    }

    private void createConnections(int numConnections) throws SQLException {
        // 循环创建指定数目的数据库连接
        for (int x = 0; x < numConnections; x++) {
            // 是否连接池中的数据库连接的数量己经达到最大？最大值由类成员 maxConnections
            // 指出，假如 maxConnections 为 0 或负数，表示连接数量没有限制。
            // 假如连接数己经达到最大，即退出。
            if (this.maxConnections > 0
                    && this.connections.size() >= this.maxConnections) {
                break;
            }
            // add a new PooledConnection object to connections vector
            // 增加一个连接到连接池中（向量 connections 中）
            try {
                connections.addElement(new PooledConnection(newConnection()));// connections中放置的是Connection对象
            } catch (SQLException e) {
                logger.error("Database connection pool creation failed！", e);
            }
            logger.info("Database connection pool being created ......！");
        }
    }

    private Connection newConnection() throws SQLException {
        Connection conn = null;
        // 创建一个数据库连接
        if ("SIMPLE".equalsIgnoreCase(authentication)) {
            conn = getSimpleConnection();
        } else if ("LDAP".equalsIgnoreCase(authentication)) {
            conn = getLDAPConnection();
        } else if ("KERBEROS".equalsIgnoreCase(authentication)) {
            conn = getKerberosConnection();
        } else {
            logger.error("Invalid authentication: " + authentication);
            return null;
        }
        // 假如这是第一次创建数据库连接，即检查数据库，获得此数据库答应支持的
        // 最大客户连接数目
        // connections.size()==0 表示目前没有连接己被创建
        if (connections.size() == 0) {
            // 检查数据库的操作只需要做一次就可以了
            DatabaseMetaData metaData = conn.getMetaData();
            int driverMaxConnections = metaData.getMaxConnections();
            // 数据库返回的 driverMaxConnections 若为 0 ，表示此数据库没有最大
            // 连接限制，或数据库的最大连接限制不知道
            // driverMaxConnections 为返回的一个整数，表示此数据库答应客户连接的数目
            // 假如连接池中设置的最大连接数量大于数据库答应的连接数目 , 则置连接池的最大
            // 连接数目为数据库答应的最大数目
            if (driverMaxConnections > 0
                    && this.maxConnections > driverMaxConnections) {
                this.maxConnections = driverMaxConnections;
            }
        }
        return conn; // 返回创建的新的数据库连接
    }

    public synchronized Connection getConnection() throws SQLException {
        // 确保连接池己被创建
        if (connections == null) {
            System.out.println("连接池还没创建!");
            return null; // 连接池还没创建，则返回 null
        }
        // 如果连接池已近创建，那么从链接池中获取一个可用的数据库链接

        Connection conn = getFreeConnection(); // 获得一个可用的数据库连接
        // 假如目前没有可以使用的连接，即所有的连接都在使用中
        while (conn == null) {
            // 等一会再试
            wait(250);
            conn = getFreeConnection(); // 重新再试，直到获得可用的连接，假如
            // getFreeConnection() 返回的为 null
            // 则表明创建一批连接后也不可获得可用连接
        }
        return conn; // 返回获得的可用的连接
    }

    private Connection getFreeConnection() throws SQLException {
        // 从连接池中获得一个可用的数据库连接
        Connection conn = findFreeConnection();
        if (conn == null) {
            // 假如目前连接池中没有可用的连接
            // 创建一些连接
            createConnections(incrementalConnections);
            // 重新从池中查找是否有可用连接
            conn = findFreeConnection();
            if (conn == null) {
                // 假如创建连接后仍获得不到可用的连接，则返回 null
                return null;
            }
        }
        return conn;
    }

    private Connection findFreeConnection() throws SQLException {
        Connection conn = null;
        PooledConnection pConn = null;
        // 获得连接池向量中所有的对象
        /**
         * boolean hasMoreElemerts()
         * 测试Enumeration枚举对象中是否还含有元素，如果返回true，则表示还含有至少一个的元素。 ·Object
         * nextElement() ：如果Bnumeration枚举对象还含有元素，该方法得到对象中的下一个元素。
         */
        Enumeration<PooledConnection> enumerate = connections.elements();
        // 遍历所有的对象，看是否有可用的连接
        while (enumerate.hasMoreElements()) {
            pConn = (PooledConnection) enumerate.nextElement();
            if (!pConn.isBusy()) {
                // 假如此对象不忙，则获得它的数据库连接并把它设为忙
                conn = pConn.getConnection();
                pConn.setBusy(true);
                // 测试此连接是否可用
                if (!testConnection(conn)) {
                    // 假如此连接不可再用了，则创建一个新的连接，
                    // 并替换此不可用的连接对象，假如创建失败，返回 null
                    try {
                        conn = newConnection();
                    } catch (SQLException e) {
                        logger.error("创建数据库连接失败！ ", e);
                    }
                    pConn.setConnection(conn);
                }
                break; // 己经找到一个可用的连接，退出
            }
        }
        return conn; // 返回找到到的可用连接
    }

    private boolean testConnection(Connection conn) {
        try {
            // 判定测试表是否存在
            if (testTable.equals("")) {
                // 假如测试表为空，试着使用此连接的 setAutoCommit() 方法
                // 来判定连接否可用（此方法只在部分数据库可用，假如不可用 ,
                // 抛出异常）。注重：使用测试表的方法更可靠
                conn.setAutoCommit(true);
            } else { // 有测试表的时候使用测试表测试
                Statement stmt = conn.createStatement();
                stmt.executeQuery("select count(*) from " + testTable);
            }
        } catch (SQLException e) {
            // 上面抛出异常，此连接己不可用，关闭它，并返回 false;
            closeConnection(conn);
            return false;
        }
        // 连接可用，返回 true
        return true;
    }

    public void returnConnection(Connection conn) {
        // 确保连接池存在，假如连接没有创建（不存在），直接返回
        if (connections == null) {
            // 连接池是连接的归宿，没有连接池的时候，任务执行完不用的连接就无法归还给连接池，造成连接没有人管理了，造成浪费。

            System.out.println(" 连接池不存在，无法返回此连接到连接池中 !");
            return;
        }
        PooledConnection pConn = null;
        Enumeration<PooledConnection> enumerate = connections.elements();
        // 遍历连接池中的所有连接，找到这个要返回的连接对象
        while (enumerate.hasMoreElements()) {
            pConn = (PooledConnection) enumerate.nextElement();
            // 先找到连接池中的要返回的连接对象
            if (conn == pConn.getConnection()) {
                // 经典：连接被用于去执行任务，并不是说连接不再连接池中了，而是在连接池中的状态改变了，即由空闲状态变为忙碌状态

                // 找到了 , 设置此连接为空闲状态
                pConn.setBusy(false);
                break;
            }
        }
    }

    public synchronized void refreshConnections() throws SQLException {
        // 刷新连接的目的在于当某一链接卡死的时候用一个新的链接取代他
        // 确保连接池己创新存在
        if (connections == null) {
            System.out.println(" 连接池不存在，无法刷新 !");
            return;
        }
        PooledConnection pConn = null;
        Enumeration<PooledConnection> enumerate = connections.elements();
        while (enumerate.hasMoreElements()) {
            // 获得一个连接对象
            pConn = (PooledConnection) enumerate.nextElement();
            // 假如对象忙则等 5 秒 ,5 秒后直接刷新
            if (pConn.isBusy()) {
                wait(5000); // 等 5 秒
            }
            // 关闭此连接，用一个新的连接代替它。
            closeConnection(pConn.getConnection());
            pConn.setConnection(newConnection());
            pConn.setBusy(false);
        }
    }

    public synchronized void closeConnectionPool() throws SQLException {
        // 确保连接池存在，假如不存在，返回
        if (connections == null) {
            System.out.println(" 连接池不存在，无法关闭 !");
            return;
        }
        PooledConnection pConn = null;
        Enumeration<PooledConnection> enumerate = connections.elements();
        while (enumerate.hasMoreElements()) {
            pConn = (PooledConnection) enumerate.nextElement();
            // 假如忙，等 5 秒
            if (pConn.isBusy()) {
                wait(5000); // 等 5 秒
            }
            // 5 秒后直接关闭它
            closeConnection(pConn.getConnection());
            // 从连接池向量中删除它
            connections.removeElement(pConn);
        }
        // 置连接池为空
        connections = null;
    }

    private void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            logger.error(" 关闭数据库连接出错： ", e);
        }
    }

    private void wait(int mSeconds) {
        try {
            Thread.sleep(mSeconds);
        } catch (InterruptedException e) {
            logger.error("Thread sleep error: ", e);
        }
    }
}
