package org.mariadb.jdbc.failover;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.*;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    @Test
    public void testManualFailover() throws SQLException, InterruptedException {
        Assume.assumeTrue(initialAuroraUrl != null);
        Connection connection = null;
        try {
            connection = getNewConnection("&secondsBeforeRetryMaster=2", false);
            Assert.assertFalse(connection.isReadOnly());
            long beginTest = System.currentTimeMillis();
            boolean waitToSwitch = true;

            while(waitToSwitch) {
                Thread.sleep(1000);
                try {
                    waitToSwitch = !connection.isReadOnly();
                }catch (SQLException e) {e.printStackTrace();}
            }
            log.fine("failover automatically done after "+((System.currentTimeMillis() - beginTest)));
            boolean waitTobackMaster = true;
            while(waitTobackMaster) {
                Thread.sleep(1000);
                try {
                    waitTobackMaster = connection.isReadOnly();
                }catch (SQLException e) {e.printStackTrace();}
            }
            log.fine("return on master automatically done after "+((System.currentTimeMillis() - beginTest)));


        } finally {
            if(connection!=null) connection.close();
        }
    }*/
}
