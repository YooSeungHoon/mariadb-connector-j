package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

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

    /*@Test
    public void randomConnection() throws SQLException {
        Assume.assumeTrue(initialGaleraUrl != null);
        log.fine("randomConnection begin");
        try {
            Connection connection;
            Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
            for (int i = 0; i < 20; i++) {
                connection = getNewConnection(false);
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW SESSION VARIABLES LIKE 'wsrep_node_name'");
                rs.next();
                String currentNodeName = rs.getString(2);
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
    }*/

    @Test
    public void checkBlacklist() throws SQLException, InterruptedException {
        Assume.assumeTrue(initialGaleraUrl != null);
        Connection connection = null;
        log.fine("checkBlacklist begin");
        try {
            connection = getNewConnection(true);
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'wsrep_node_name'");
            rs.next();
            String initialNodeName = rs.getString(2);

            //The node must be configure with specific names :
            //node x:wsrep_node_name = "galerax"
            int currentProxy = Integer.parseInt(initialNodeName.substring(6));
            stopProxy(currentProxy, 20000);

            try{
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //normal exception
            }
            //wait for reconnection
            Thread.sleep(15000);

            rs = st.executeQuery("SHOW SESSION VARIABLES LIKE 'wsrep_node_name'");

            rs.next();
            String newNodeName = rs.getString(2);
            Assert.assertFalse(newNodeName.equals(initialNodeName));
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                Assert.assertTrue(protocol.getProxy().listener.getBlacklist().size() == 1);
            } catch (Throwable e) {
                Assert.fail();
            }
        } finally {
            log.fine("checkBlacklist done");
            Thread.sleep(10000);
            if (connection != null) connection.close();
        }

    }

    class MutableInt {
        int value = 1; // note that we start at 1 since we're counting
        public void increment () { ++value;      }
        public int  get ()       { return value; }
    }
}
