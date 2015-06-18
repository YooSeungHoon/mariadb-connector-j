package org.mariadb.jdbc.failover;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.BaseTest;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;

public class OldFailoverTest extends BaseTest {

    /**
     * check old connection way before multihost was handle
     * @throws Exception
     */
    @Test
    public void isOldConfigurationValid() throws Exception {
        String falseUrl = "jdbc:mysql://localhost:1111," + hostname + ":" + port + "/" + database+"?user=" + username
                + (password != null && !"".equals(password) ? "&password=" + password : "")
                + (parameters != null ? parameters : "");

        try {
            //the first host doesn't exist, so with the random host selection, verifying that we connect to the good host
            for (int i=0;i<10;i++) {
                Connection tmpConnection = openNewConnection(falseUrl);
                Statement tmpStatement = tmpConnection.createStatement();
                tmpStatement.execute("SELECT 1");
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

}
