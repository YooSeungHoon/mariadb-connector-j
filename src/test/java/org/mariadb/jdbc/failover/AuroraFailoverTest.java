package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AuroraFailoverTest extends BaseMultiHostTest {

    @Before
    public void init() throws SQLException {
        initialUrl = initialAuroraUrl;
        proxyUrl = proxyAuroraUrl;
    }
/*

    class MutableInt {
        int value = 1; // note that we start at 1 since we're counting
        public void increment () { ++value;      }
        public int  get ()       { return value; }
    }


    @Test
    public void testWriteOnMaster() throws SQLException {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("testWriteOnMaster begin");
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
            log.fine("testWriteOnMaster OK");
        } finally {
            log.fine("testWriteOnMaster done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testErrorWriteOnReplica() throws SQLException {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("testErrorWriteOnReplica begin");
        try {
            connection = getNewConnection(false);
            connection.setReadOnly(true);
            Statement stmt = connection.createStatement();
            Assert.assertTrue(connection.isReadOnly());
            try {
                stmt.execute("drop table  if exists multinode4");
                log.severe("ERROR - > must not be able to write on slave --> check if you database is start with --read-only");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("testErrorWriteOnReplica done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void testReplication() throws SQLException, InterruptedException{
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("testReplication begin");
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinodeReadSlave");
            stmt.execute("create table multinodeReadSlave (id int not null primary key auto_increment, test VARCHAR(10))");

            //wait to be sure slave have replicate data
            Thread.sleep(2000);

            connection.setReadOnly(true);

            ResultSet rs = stmt.executeQuery("Select count(*) from multinodeReadSlave");
            Assert.assertTrue(rs.next());
        } finally {
            log.fine("testReplication done");
            if (connection != null) connection.close();
        }
    }


    @Test
    public void randomConnection() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        log.fine("randomConnection begin");
        try {
            Connection connection;
            Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
            int masterId = -1;
            for (int i = 0; i < 20; i++) {
                connection = getNewConnection(false);
                int serverId = getAuroraServerId(connection);
                log.fine("master server found " + serverId);
                if (i>0) Assert.assertTrue(masterId == serverId);
                masterId = serverId;
                connection.setReadOnly(true);
                int replicaId = getAuroraServerId(connection);
                log.fine("++++++++++++slave  server found " + replicaId);
                MutableInt count = connectionMap.get(replicaId);
                if (count == null) {
                    connectionMap.put(String.valueOf(replicaId), new MutableInt());
                } else {
                    count.increment();
                }
                connection.close();
            }

            Assert.assertTrue(connectionMap.size() >= 2 );
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
    public void failoverSlaveToMaster() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverSlaveToMaster begin");
        int slaveServerId = -1;
        try {
            connection = getNewConnection(true);
            connection.setReadOnly(true);
            slaveServerId = getAuroraServerId(connection);

            stopProxy(slaveServerId);

            int masterServerId = getAuroraServerId(connection);

            Assert.assertFalse(slaveServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            restartProxy(slaveServerId);
            assureBlackList(connection);
            if (connection != null) connection.close();
            log.fine("failoverSlaveToMaster done");
        }
    }


    @Test
    public void failoverSlaveToMasterFail() throws Throwable{
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverSlaveToMaster begin");
        try {
            connection = getNewConnection("&secondsBeforeRetryMaster=1",true);
            int masterServerId = getAuroraServerId(connection);
            connection.setReadOnly(true);
            int slaveServerId = getAuroraServerId(connection);
            Assert.assertTrue(slaveServerId != masterServerId);
            stopProxy(masterServerId);
            try {
                //must not throw error until there is a query
                connection.setReadOnly(false);
                Assert.fail();
            } catch (SQLException e) {
            }
            int currentServerId = getAuroraServerId(connection);
            Assert.assertTrue(slaveServerId == currentServerId);
            Assert.assertTrue(connection.isReadOnly());
            restartProxy(masterServerId);
            Thread.sleep(2000);

            //failover must have back uo to master
            currentServerId = getAuroraServerId(connection);
            Assert.assertTrue(masterServerId == currentServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
            log.fine("failoverSlaveToMaster done");
        }
    }

    @Test
    public void failoverDuringMasterSetReadOnly() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverDuringMasterSetReadOnly begin");
        int masterServerId=-1;
        try {
            connection = getNewConnection(true);
            masterServerId = getAuroraServerId(connection);

            stopProxy(masterServerId);

            connection.setReadOnly(true);
            int slaveServerId = getAuroraServerId(connection);

            Assert.assertFalse(slaveServerId == masterServerId);
            Assert.assertTrue(connection.isReadOnly());
        } finally {
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
            log.fine("failoverDuringMasterSetReadOnly done");
        }
    }

    @Test
    public void failoverDuringSlaveSetReadOnly() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverSlaveToMaster begin");
        int slaveServerId=-1;
        try {
            connection = getNewConnection(true);
            connection.setReadOnly(true);
            slaveServerId = getAuroraServerId(connection);

            stopProxy(slaveServerId, 2000);

            connection.setReadOnly(false);

            int masterServerId = getAuroraServerId(connection);

            Assert.assertFalse(slaveServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
            log.fine("failoverSlaveToMaster done");
        }
    }



    @Test()
    public void failoverSlaveAndMasterWithoutAutoConnect() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverSlaveAndMasterWithoutAutoConnect begin");
        int firstSlaveId =-1;
        int masterServerId = -1;
        try {
            connection = getNewConnection(true);
            masterServerId = getAuroraServerId(connection);
            log.fine("master server_id = " + masterServerId);
            connection.setReadOnly(true);
            firstSlaveId = getAuroraServerId(connection);
            log.fine("slave1 server_id = " + firstSlaveId);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            //must throw an error, because not in autoreconnect Mode
            try {
                connection.createStatement().executeQuery("SELECT CONNECTION_ID()");
                Assert.fail();
            } catch (SQLException e) {
                Assert.assertTrue(true);
            }
        } finally {
            log.fine("failoverSlaveAndMasterWithoutAutoConnect done");
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverSlaveAndMasterWithAutoConnect() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverSlaveAndMasterWithAutoConnect begin");
        int firstSlaveId =-1;
        int masterServerId = -1;
        try {
            connection = getNewConnection("&autoReconnect=true", true);

            //search actual server_id for master and slave
            masterServerId = getAuroraServerId(connection);
            log.fine("master server_id = " + masterServerId);

            connection.setReadOnly(true);

            firstSlaveId = getAuroraServerId(connection);
            log.fine("slave1 server_id = " + firstSlaveId);

            stopProxy(masterServerId);
            stopProxy(firstSlaveId);

            //must reconnect to the second slave without error
            int currentSlaveId = getAuroraServerId(connection);
            log.fine("currentSlaveId server_id = " + currentSlaveId);
            Assert.assertTrue(currentSlaveId != firstSlaveId);
            Assert.assertTrue(currentSlaveId != masterServerId);
        } finally {
            log.fine("failoverSlaveAndMasterWithAutoConnect done");
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverMasterWithAutoConnect() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverMasterWithAutoConnect begin");
        try {
            connection = getNewConnection("&autoReconnect=true", true);
            int masterServerId = getAuroraServerId(connection);

            stopProxy(masterServerId, 100);
            //with autoreconnect, the connection must reconnect automatically
            int currentServerId = getAuroraServerId(connection);

            Assert.assertTrue(currentServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            log.fine("failoverMasterWithAutoConnect done");
            assureProxy();
            assureBlackList(connection);
            try {
                Thread.sleep(200); //wait to not have problem with next test
            } catch (InterruptedException e) {
            }
            if (connection != null) connection.close();
        }
    }

    @Test
    public void checkReconnectionToMasterAfterTimeout() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("checkReconnectionToMasterAfterTimeout begin");
        int masterServerId=-1;
        try {
            connection = getNewConnection("&secondsBeforeRetryMaster=1", true);
            masterServerId = getAuroraServerId(connection);
            stopProxy(masterServerId);
            try {
                connection.createStatement().execute("SELECT 1");
            } catch (Exception e) {
            }
            restartProxy(masterServerId);
            //wait for more than the 1s (secondsBeforeRetryMaster) timeout, to check that master is on
            Thread.sleep(3000);

            int currentServerId = getAuroraServerId(connection);

            Assert.assertTrue(currentServerId == masterServerId);
            Assert.assertFalse(connection.isReadOnly());
        } finally {
            log.fine("checkReconnectionToMasterAfterTimeout done");
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
        }
    }


    @Test
    public void checkReconnectionToMasterAfterQueryNumber() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("checkReconnectionToMasterAfterQueryNumber begin");
        try {
            connection = getNewConnection("&autoReconnect=true&secondsBeforeRetryMaster=3000&queriesBeforeRetryMaster=10", true);
            int masterServerId = getAuroraServerId(connection);
            stopProxy(masterServerId);

            for (int i = 0; i < 10; i++) {
                try {
                    int currentServerId = getAuroraServerId(connection);
                    Assert.assertFalse(currentServerId == masterServerId);
                } catch (SQLException e) {
                    //must be on read-only connection, so musn't throw an exception
                    Assert.fail();
                }
            }

            restartProxy(masterServerId);

            //give time to autoreconnect to master
            Thread.sleep(15000);
            int currentServerId = getAuroraServerId(connection);
            Assert.assertTrue(currentServerId == masterServerId);
        } finally {

            log.fine("checkReconnectionToMasterAfterQueryNumber done");
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
        }
    }

    @Test
    public void writeToSlaveAfterFailover() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("writeToSlaveAfterFailover begin");
        try {
            connection = getNewConnection(true);
            //if super user can write on slave
            Assume.assumeTrue(!hasSuperPrivilege(connection, "writeToSlaveAfterFailover"));
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinode2");
            st.execute("create table multinode2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            st.execute("insert into multinode2 (id, amount) VALUE (1 , 100)");

            int masterServerId = getAuroraServerId(connection);

            stopProxy(masterServerId);
            try {
                st.execute("insert into multinode2 (id, amount) VALUE (2 , 100)");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("writeToSlaveAfterFailover done");
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
        }

    }
*/
    @Test
    public void checkReconnectionAfterInactivity() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("checkReconnectionAfterInactivity begin");
        try {
            connection = getNewConnection("&validConnectionTimeout=1&secondsBeforeRetryMaster=4", true);
            Statement st = connection.createStatement();
            st.execute("drop table  if exists multinodeTransaction1");
            st.execute("create table multinodeTransaction1 (id int not null primary key , amount int not null) ENGINE = InnoDB");

            int masterServerId = getAuroraServerId(connection);

            stopProxy(masterServerId);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            log.fine("must be on a slave connection");
            //must have failover to slave connection
            Assert.assertTrue(connection.isReadOnly());
            restartProxy(masterServerId);

            ResultSet rs = connection.createStatement().executeQuery("select server_id from information_schema.replica_host_status where session_id = 'MASTER_SESSION_ID'");
            rs.next();
            //wait for more than the 4s (secondsBeforeRetryMaster) timeout, to check that master is on
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
            }

            //must have found back the master
            log.fine("must be on the master connection");
            Assert.assertFalse(connection.isReadOnly());
            log.fine("checkReconnectionAfterInactivity done");

        } finally {
            log.fine("checkReconnectionAfterInactivity done");
            assureProxy();
            assureBlackList(connection);
            if (connection != null) connection.close();
        }
    }
/*
    @Test()
    public void checkNoSwitchConnectionDuringTransaction() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("checkNoSwitchConnectionDuringTransaction begin");
        try {
            connection = getNewConnection("&autoReconnect=true", false);
            Statement st = connection.createStatement();

            st.execute("drop table  if exists multinodeTransaction2");
            st.execute("create table multinodeTransaction2 (id int not null primary key , amount int not null) ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction2 (id, amount) VALUE (1 , 100)");

            try {
                //in transaction, so must trow an error
                connection.setReadOnly(true);
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("checkNoSwitchConnectionDuringTransaction done");
            if (connection != null) connection.close();
        }
    }

    @Test
    public void failoverMasterWithAutoConnectAndTransaction() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("failoverMasterWithAutoConnectAndTransaction begin");
        try {
            connection = getNewConnection("&autoReconnect=true", true);
            Statement st = connection.createStatement();

            int masterServerId = getAuroraServerId(connection);
            st.execute("drop table  if exists multinodeTransaction");
            st.execute("create table multinodeTransaction (id int not null primary key , amount int not null) ENGINE = InnoDB");
            connection.setAutoCommit(false);
            st.execute("insert into multinodeTransaction (id, amount) VALUE (1 , 100)");
            st.execute("insert into multinodeTransaction (id, amount) VALUE (2 , 200)");
            st.execute("START TRANSACTION");
            st.execute("update multinodeTransaction set amount = amount+100");
            st.execute("insert into multinodeTransaction (id, amount) VALUE (3 , 10)");
            stopProxy(masterServerId);
            try {
                //with autoreconnect but in transaction, query must throw an error
                st.execute("insert into multinodeTransaction (id, amount) VALUE (4 , 10)");
                Assert.fail();
            } catch (SQLException e) { }
        } finally {
            log.fine("failoverMasterWithAutoConnectAndTransaction done");
            assureProxy();
            if (connection != null) {
                assureBlackList(connection);
                try { connection.setAutoCommit(true); } catch (SQLException e) {}
                connection.close();
            }
        }
    }

    @Test
    public void testSynchronizedReadOnly() throws Throwable {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        log.fine("testSynchronizedReadOnly begin");
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multisync");
            stmt.execute("create table multisync (id int not null primary key , amount int not null) ENGINE = InnoDB");
            stmt.execute("INSERT INTO multisync (id, amount) values (1, 0)");
            long currentTime = System.currentTimeMillis();
            ExecutorService exec= Executors.newFixedThreadPool(2);
            exec.execute(new ChangeAmount(connection, 100));
            exec.execute(new ChangeAmount(connection, 100));
            //wait for thread endings
            exec.shutdown();
            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) { }
            log.info("total time : "+ (System.currentTimeMillis() - currentTime));
            ResultSet rs = stmt.executeQuery("SELECT amount FROM multisync");
            rs.next();
            log.fine(" total result :" + rs.getInt(1));
            Assert.assertTrue(200 == rs.getInt(1));
        } finally {
            log.fine("testSynchronizedReadOnly done");
            if (connection != null) connection.close();
        }
    }

    protected class ChangeAmount implements Runnable {
        Connection connection;
        int changeAmount;
        public ChangeAmount(Connection connection, int changeAmount) {
            this.connection = connection;
            this.changeAmount = changeAmount;
        }

        public void run() {
            try {
                Statement st = connection.createStatement();
                for (int i = 1; i <= changeAmount; i++) {
                    st.execute("UPDATE  multisync set amount = amount + 1");
                    if (i%200==0)log.fine("update : "+i);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }*/
}
