package org.mariadb.jdbc.multihost;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.mysql.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.logging.*;

/**
 *  Base util class.
 *  For testing
 *  mvn test  -Pmultihost -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root&favor=master-slave -DlogLevel=FINEST
 */
@Ignore
public class BaseMultiHostTest {
    protected static Logger log = Logger.getLogger("org.maria.jdbc");
    protected static String url;
    protected static String auroraUrl;
    protected static boolean multihostUrlOk = false;
    protected static boolean auroraUrlOk = false;
    protected static String username;
    private static String hostname;
    //hosts
    protected static TcpProxy[] tcpProxies;

    @BeforeClass
    public static void beforeClass()  throws SQLException, IOException {
        //get the multi-host connection string
        String tmpUrl = System.getProperty("defaultMultiHostUrl");
        String tmpUrlAurora = System.getProperty("defaultAuroraHostUrl");
        if (tmpUrl != null) multihostUrlOk=true;
        if (tmpUrlAurora != null) auroraUrlOk=true;
        String logLevel = System.getProperty("logLevel");
        if (logLevel != null) {
            if (log.getHandlers().length == 0) {
                ConsoleHandler consoleHandler = new ConsoleHandler();
                consoleHandler.setFormatter(new CustomFormatter());
                consoleHandler.setLevel(Level.parse(logLevel));
                log.addHandler(consoleHandler);
                log.setLevel(Level.FINE);

                Logger.getLogger(MultiHostListener.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(MultiHostListener.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(AuroraListener.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(AuroraListener.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(FailoverProxy.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(FailoverProxy.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(AuroraMultiNodesProtocol.class.getName()).setLevel(Level.FINE);
                Logger.getLogger(AuroraMultiNodesProtocol.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(MySQLProtocol.class.getName()).setLevel(Level.FINE);
                Logger.getLogger(MySQLProtocol.class.getName()).addHandler(consoleHandler);
                Logger.getLogger(BaseFailoverListener.class.getName()).setLevel(Level.ALL);
                Logger.getLogger(BaseFailoverListener.class.getName()).addHandler(consoleHandler);


            }
        }

        int beginSocketPort = 52360;
        if (multihostUrlOk) url=createProxies(tmpUrl, beginSocketPort);
        if (auroraUrlOk) auroraUrl=tmpUrlAurora; //createProxies(tmpUrlAurora, beginSocketPort);

    }

    private static String createProxies(String tmpUrl, int beginSocketPort) {
        JDBCUrl tmpJdbcUrl = JDBCUrl.parse(tmpUrl);
        tcpProxies = new TcpProxy[tmpJdbcUrl.getHostAddresses().length];
        username = tmpJdbcUrl.getUsername();
        hostname = tmpJdbcUrl.getHostAddresses()[0].host;
        String sockethosts = "";
        for (int i=0;i<tmpJdbcUrl.getHostAddresses().length;i++) {
            log.info("creating socket "+tmpJdbcUrl.getHostAddresses()[i].host+":"+tmpJdbcUrl.getHostAddresses()[i].port+" -> localhost:"+beginSocketPort);
            tcpProxies[i] = new TcpProxy(tmpJdbcUrl.getHostAddresses()[i].host,
                    tmpJdbcUrl.getHostAddresses()[i].port,
                    beginSocketPort);
            sockethosts+=",localhost:"+beginSocketPort;
            beginSocketPort++;
        }

        return "jdbc:mysql://"+sockethosts.substring(1)+"/"+tmpUrl.split("/")[3];
    }

    protected Connection getNewConnection() throws SQLException {
        return getNewConnection(null);
    }

    protected Connection getNewConnection(String additionnalConnectionData) throws SQLException {
        if (additionnalConnectionData == null) {
            return DriverManager.getConnection(url);
        } else {
            return DriverManager.getConnection(url+additionnalConnectionData);
        }
    }

    protected Connection getAuroraNewConnection() throws SQLException {
        return getAuroraNewConnection(null);
    }

    protected Connection getAuroraNewConnection(String additionnalConnectionData) throws SQLException {
        if (additionnalConnectionData == null) {
            return DriverManager.getConnection(auroraUrl);
        } else {
            return DriverManager.getConnection(auroraUrl+additionnalConnectionData);
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
