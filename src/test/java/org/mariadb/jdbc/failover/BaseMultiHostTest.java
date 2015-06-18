package org.mariadb.jdbc.failover;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.MySQLConnection;
import org.mariadb.jdbc.internal.common.UrlHAMode;
import org.mariadb.jdbc.internal.mysql.*;
import org.mariadb.jdbc.internal.mysql.listener.AuroraListener;
import org.mariadb.jdbc.internal.mysql.listener.BaseListener;
import org.mariadb.jdbc.internal.mysql.listener.FailoverListener;
import org.mariadb.jdbc.internal.mysql.listener.ReplicationListener;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.logging.*;

/**
 *  Base util class.
 *  For testing
 *  example mvn test -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root -DlogLevel=FINEST
 *
 *  specific parameters :
 *  defaultMultiHostUrl :
 *
 *
 */
@Ignore
public class BaseMultiHostTest {
    protected static Logger log = Logger.getLogger("org.maria.jdbc");

    protected static String initialGaleraUrl;
    protected static String initialAuroraUrl;
    protected static String initialReplicationUrl;
    protected static String initialUrl;

    protected static String proxyGaleraUrl;
    protected static String proxyAuroraUrl;
    protected static String proxyReplicationUrl;
    protected static String proxyUrl;

    protected static String username;
    private static String hostname;

    //hosts
    private static TcpProxy[] tcpProxies;

    @BeforeClass
    public static void beforeClass()  throws SQLException, IOException {

        initialGaleraUrl = System.getProperty("defaultGaleraUrl");
        initialReplicationUrl = System.getProperty("defaultReplicationUrl");
        initialAuroraUrl = System.getProperty("defaultAuroraHostUrl");

        String logLevel = System.getProperty("logLevel");
        if (logLevel != null) {
            if (log.getHandlers().length == 0) {
                ConsoleHandler consoleHandler = new ConsoleHandler();
                consoleHandler.setFormatter(new CustomFormatter());
                consoleHandler.setLevel(Level.parse(logLevel));
                log.addHandler(consoleHandler);
                log.setLevel(Level.FINE);

                Logger.getLogger(ReplicationListener.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(ReplicationListener.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(AuroraListener.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(AuroraListener.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(FailoverProxy.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(FailoverProxy.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(AuroraMultiNodesProtocol.class.getName()).setLevel(Level.FINE);
                Logger.getLogger(AuroraMultiNodesProtocol.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(MySQLProtocol.class.getName()).setLevel(Level.FINE);
                Logger.getLogger(MySQLProtocol.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(BaseListener.class.getName()).setLevel(Level.FINE);
                Logger.getLogger(BaseListener.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(FailoverListener.class.getName()).setLevel(Level.FINE);
                Logger.getLogger(FailoverListener.class.getName()).addHandler(consoleHandler);

            }
        }

        if (initialReplicationUrl != null) proxyReplicationUrl=createProxies(initialReplicationUrl);
        if (initialGaleraUrl != null) proxyGaleraUrl=createProxies(initialGaleraUrl);
        if (initialAuroraUrl != null) proxyAuroraUrl=createProxies(initialAuroraUrl);
    }

    private static String createProxies(String tmpUrl) {
        JDBCUrl tmpJdbcUrl = JDBCUrl.parse(tmpUrl);
        tcpProxies = new TcpProxy[tmpJdbcUrl.getHostAddresses().size()];
        username = tmpJdbcUrl.getUsername();
        hostname = tmpJdbcUrl.getHostAddresses().get(0).host;
        String sockethosts = "";
        for (int i=0;i<tmpJdbcUrl.getHostAddresses().size();i++) {
            try {
                tcpProxies[i] = new TcpProxy(tmpJdbcUrl.getHostAddresses().get(i).host, tmpJdbcUrl.getHostAddresses().get(i).port);
                log.info("creating socket "+tmpJdbcUrl.getHostAddresses().get(i).host+":"+tmpJdbcUrl.getHostAddresses().get(i).port+" -> localhost:"+tcpProxies[i].getLocalPort());
                sockethosts+=",localhost:"+tcpProxies[i].getLocalPort();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if (tmpJdbcUrl.getHaMode().equals(UrlHAMode.NONE)) {
            return "jdbc:mysql://"+sockethosts.substring(1)+"/"+tmpUrl.split("/")[3];
        } else {
            return "jdbc:mysql:"+tmpJdbcUrl.getHaMode()+"://"+sockethosts.substring(1)+"/"+tmpUrl.split("/")[3];
        }

    }
    protected Connection getNewConnection(boolean proxy) throws SQLException {
        return getNewConnection(null, proxy);
    }

    protected Connection getNewConnection(String additionnalConnectionData, boolean proxy) throws SQLException {
        if (proxy) {
            if (additionnalConnectionData == null) {
                return DriverManager.getConnection(proxyUrl);
            } else {
                return DriverManager.getConnection(proxyUrl + additionnalConnectionData);
            }
        } else {
            if (additionnalConnectionData == null) {
                return DriverManager.getConnection(initialUrl);
            } else {
                return DriverManager.getConnection(initialUrl + additionnalConnectionData);
            }
        }
    }

    @AfterClass
    public static void afterClass()  throws SQLException {
        if (tcpProxies !=null) {
            for (TcpProxy tcpProxy : tcpProxies) {
                try {
                    tcpProxy.stop();
                } catch (Exception e) {}
            }
        }
    }
    public void stopProxy(int hostNumber, long millissecond) {
        log.fine("stopping host "+hostNumber);
        tcpProxies[hostNumber - 1].restart(millissecond);
    }



    //does the user have super privileges or not?
    public boolean hasSuperPrivilege(Connection connection, String testName) throws SQLException{
        boolean superPrivilege = false;
        Statement st = connection.createStatement();

        // first test for specific user and host combination
        ResultSet rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '" + hostname + "'");
        if (rs.next()) {
            superPrivilege = (rs.getString(1).equals("Y") ? true : false);
        } else
        {
            // then check for user on whatever (%) host
            rs = st.executeQuery("SELECT Super_Priv FROM mysql.user WHERE user = '" + username + "' AND host = '%'");
            if (rs.next())
                superPrivilege = (rs.getString(1).equals("Y") ? true : false);
        }

        rs.close();

        if (superPrivilege)
            log.info("test '" + testName + "' skipped because user '" + username + "' has SUPER privileges");

        return superPrivilege;
    }

    protected Protocol getProtocolFromConnection(Connection conn) throws Throwable {

        Method getProtocol = MySQLConnection.class.getDeclaredMethod("getProtocol", new Class[0]);
        getProtocol.setAccessible(true);
        return (Protocol) getProtocol.invoke(conn);
    }
}
class CustomFormatter  extends Formatter {
    private static final String format = "[%1$tT] %4$s: %2$s - %5$s %6$s%n";
    private final java.util.Date dat = new java.util.Date();
    public synchronized String format(LogRecord record) {
        dat.setTime(record.getMillis());
        String source;
        if (record.getSourceClassName() != null) {
            source = record.getSourceClassName();
            if (record.getSourceMethodName() != null) {
                source += " " + record.getSourceMethodName();
            }
        } else {
            source = record.getLoggerName();
        }
        String message = formatMessage(record);
        String throwable = "";
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            throwable = sw.toString();
        }
        return String.format(format,
                dat,
                source,
                record.getLoggerName(),
                record.getLevel().getName(),
                message,
                throwable);
    }
}
