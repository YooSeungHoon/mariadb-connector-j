package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *  test for galera
 *  The node must be configure with specific names :
 *  node 1 : wsrep_node_name = "galera1"
 *  ...
 *  node x : wsrep_node_name = "galerax"
 *  exemple mvn test  -DdbUrl=jdbc:mysql://localhost:3306,localhost:3307/test?user=root
 */
public class GaleraFailoverTest extends BaseMultiHostTest {

    @Before
    public void init() throws SQLException {
        initialUrl = initialGaleraUrl;
        proxyUrl = proxyGaleraUrl;
    }

    @Test
    public void randomConnection() throws SQLException {
        Assume.assumeTrue(initialGaleraUrl != null);
        log.fine("randomConnection begin");
        try {
            Connection connection;
            Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
            for (int i = 0; i < 20; i++) {
                connection = getNewConnection(false);
                String currentNodeName = getGaleraServerName(connection);
                log.fine("Server found " + currentNodeName);
                MutableInt count = connectionMap.get(currentNodeName);
                if (count == null) {
                    connectionMap.put(currentNodeName, new MutableInt());
                } else {
                    count.increment();
                }
                connection.close();
            }
            Assert.assertTrue(connectionMap.size() > 2 );
            for (String key : connectionMap.keySet()) {
                Integer connectionCount = connectionMap.get(key).get();
                Assert.assertTrue(connectionCount > 1 );
                log.fine(" ++++ Server " + key+ " : "+connectionCount+" connections ");
            }
            log.fine("randomConnection OK");
        } finally {
            log.fine("randomConnection done");
        }
    }

    @Test
    public void checkStaticBlacklist() throws SQLException, InterruptedException {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        log.fine("checkStaticBlacklist begin");
        try {
            connection = getNewConnection(true);
            Statement st = connection.createStatement();

            //The node must be configure with specific names :
            //node x:wsrep_node_name = "galerax"
            int firstServerId = getGaleraServerId(connection);

            stopProxy(firstServerId);

            try{
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //normal exception that permit to blacklist the failing connection.
            }

            //check blacklist size
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                Assert.assertTrue(protocol.getProxy().listener.getBlacklist().size() == 1);
                //replace proxified HostAddress by not proxy one
                JDBCUrl jdbcUrl = JDBCUrl.parse(initialUrl);
                protocol.getProxy().listener.getBlacklist().put(jdbcUrl.getHostAddresses().get(firstServerId - 1), System.currentTimeMillis());
            } catch (Throwable e) {
                Assert.fail();
            }

            //add first Host to blacklist
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                protocol.getProxy().listener.getBlacklist().size();
            } catch (Throwable e) {
                Assert.fail();
            }

            ExecutorService exec= Executors.newFixedThreadPool(2);
            //check blacklist shared
            for (int i=0; i<20; i++) {
                exec.execute(new CheckBlacklist(firstServerId));
            }
            //wait for thread endings
            exec.shutdown();
            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) { }

        } finally {
            log.fine("checkStaticBlacklist done");
            assureProxy();
            if (connection != null) connection.close();
        }
    }

    protected class CheckBlacklist implements Runnable {
        int firstServerId;
        public CheckBlacklist(int firstServerId) {
            this.firstServerId = firstServerId;
        }

        public void run() {
            Connection connection2 = null;
            try {
                connection2 = getNewConnection();
                int otherServerId = getGaleraServerId(connection2);
                log.fine("connected to server " + getGaleraServerName(connection2));
                Assert.assertTrue(otherServerId != firstServerId);
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail();
            } finally {
                if (connection2 != null) {
                    try {
                        connection2.close();
                    } catch (SQLException e) { e.printStackTrace(); }
                }
            }
        }
    }



    class MutableInt {
        int value = 1; // note that we start at 1 since we're counting
        public void increment () { ++value;      }
        public int  get ()       { return value; }
    }


    @Test
    public void testMultiHostWriteOnMaster() throws SQLException {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        log.fine("testMultiHostWriteOnMaster begin");
        try {
            connection = getNewConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
            log.fine("testMultiHostWriteOnMaster OK");
        } finally {
            log.fine("testMultiHostWriteOnMaster done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testMultiHostWriteOnSlave() throws SQLException {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        log.fine("testMultiHostWriteOnSlave begin");
        try {
            connection = getNewConnection();
            if (!requireMinimumVersion(connection, 10, 0)) {
                //on version > 10 use SESSION READ-ONLY, before no control
                Assume.assumeTrue(false);
            }
            String masterServerName = getGaleraServerName(connection);
            connection.setReadOnly(true);

            Assert.assertTrue(masterServerName.equals(getGaleraServerName(connection)));

            Statement stmt = connection.createStatement();
            Assert.assertTrue(connection.isReadOnly());
            try {
                stmt.execute("drop table  if exists multinode4");
                log.severe("ERROR - > must not be able to write when read only set");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("testMultiHostWriteOnMaster done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testTimeToReconnectFailover() throws SQLException, InterruptedException {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        log.fine("testTimeToReconnectFailover begin");
        int masterServerId = -1;
        try {
            connection = getNewConnection("&secondsBeforeRetryMaster=1",true);
            connection.setReadOnly(true);
            masterServerId = getGaleraServerId(connection);

            stopProxy(masterServerId);
            try {
                connection.createStatement().execute("SELECT 1");
                Assert.fail();
            } catch (SQLException e) {
                //normal error
            }
            //give time to reconnect
            Thread.sleep(5000);

            connection.createStatement().execute("SELECT 1");

            int newServerId = getServerId(connection);

            Assert.assertTrue(newServerId != masterServerId);
            Assert.assertTrue(connection.isReadOnly());
        } finally {
            assureProxy();
            if (connection != null) connection.close();
            log.fine("testTimeToReconnectFailover done");
        }
    }
}
