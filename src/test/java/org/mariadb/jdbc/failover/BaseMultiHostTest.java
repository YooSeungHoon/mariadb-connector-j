package org.mariadb.jdbc.failover;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.MySQLConnection;
import org.mariadb.jdbc.internal.common.UrlHAMode;
import org.mariadb.jdbc.internal.mysql.*;

import java.io.IOException;
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
    protected static Logger log = Logger.getLogger("org.mariadb.jdbc");

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

        if (initialReplicationUrl != null) proxyReplicationUrl=createProxies(initialReplicationUrl);
        if (initialGaleraUrl != null) proxyGaleraUrl=createProxies(initialGaleraUrl);
        if (initialAuroraUrl != null) proxyAuroraUrl=createProxies(initialAuroraUrl);
    }

    public static boolean requireMinimumVersion(Connection connection, int major, int minor) throws SQLException {
        DatabaseMetaData md = connection.getMetaData();
        int dbMajor = md.getDatabaseMajorVersion();
        int dbMinor = md.getDatabaseMinorVersion();
        return (dbMajor > major ||
                (dbMajor == major && dbMinor >= minor));
    }

    private static String createProxies(String tmpUrl) {
        JDBCUrl tmpJdbcUrl = JDBCUrl.parse(tmpUrl);
        tcpProxies = new TcpProxy[tmpJdbcUrl.getHostAddresses().size()];
        username = tmpJdbcUrl.getUsername();
        hostname = tmpJdbcUrl.getHostAddresses().get(0).host;
        String sockethosts = "";
        HostAddress hostAddress;
        for (int i=0;i<tmpJdbcUrl.getHostAddresses().size();i++) {
            try {
                hostAddress = tmpJdbcUrl.getHostAddresses().get(i);
                tcpProxies[i] = new TcpProxy(hostAddress.host, hostAddress.port);
                log.info("creating socket "+hostAddress.host+":"+hostAddress.port+" -> localhost:"+tcpProxies[i].getLocalPort());
                sockethosts+=",address=(host=localhost)(port="+tcpProxies[i].getLocalPort()+")"+((hostAddress.type != null)?"(type="+hostAddress.type+")":"");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if (tmpJdbcUrl.getHaMode().equals(UrlHAMode.NONE)) {
            return "jdbc:mysql://"+sockethosts.substring(1)+"/"+tmpUrl.split("/")[3];
        } else {
            return "jdbc:mysql:"+tmpJdbcUrl.getHaMode().toString().toLowerCase()+"://"+sockethosts.substring(1)+"/"+tmpUrl.split("/")[3];
        }

    }
    protected Connection getNewConnection(boolean proxy, boolean forceNewProxy) throws SQLException {
        return getNewConnection(null, proxy, forceNewProxy);
    }

    protected Connection getNewConnection() throws SQLException {
        return getNewConnection(null, false);
    }

    protected Connection getNewConnection(boolean proxy) throws SQLException {
        return getNewConnection(null, proxy);
    }

    protected Connection getNewConnection(String additionnalConnectionData, boolean proxy) throws SQLException {
        return getNewConnection(additionnalConnectionData, proxy, false);
    }

    protected Connection getNewConnection(String additionnalConnectionData, boolean proxy, boolean forceNewProxy) throws SQLException {
        if (proxy) {
            String tmpProxyUrl = proxyUrl;
            if (forceNewProxy) {
                tmpProxyUrl = createProxies(initialUrl);
            }
            if (additionnalConnectionData == null) {
                return DriverManager.getConnection(tmpProxyUrl);
            } else {
                return DriverManager.getConnection(tmpProxyUrl + additionnalConnectionData);
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

    public void stopProxy(int hostNumber) {
        log.fine("stopping host "+hostNumber);
        tcpProxies[hostNumber - 1].stop();
    }

    public void restartProxy(int hostNumber) {
        log.fine("restart host "+hostNumber);
        if (hostNumber != -1) tcpProxies[hostNumber - 1].restart();
    }
    public void assureProxy() {
        for (TcpProxy proxy : tcpProxies) proxy.assureProxyOk();
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

    public int getServerId(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'server_id'");
        rs.next();
        return rs.getInt(2);
    }

    public String getGaleraServerName(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'wsrep_node_name'");
        rs.next();
        return rs.getString(2);
    }

    public int getGaleraServerId(Connection connection) throws SQLException, NumberFormatException {
        return Integer.parseInt(getGaleraServerName(connection).substring(6));
    }


}