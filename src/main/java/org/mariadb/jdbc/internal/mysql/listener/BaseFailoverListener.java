package org.mariadb.jdbc.internal.mysql.listener;

/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.mysql.FailoverProxy;
import org.mariadb.jdbc.internal.mysql.HandleErrorResult;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public abstract class BaseFailoverListener implements FailoverListener {
    private final static Logger log = Logger.getLogger(BaseFailoverListener.class.getName());

    public final JDBCUrl jdbcUrl ;

    protected BaseFailoverListener(JDBCUrl jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /* =========================== Failover  parameters ========================================= */
    /**
     * Driver must recreateConnection after a failover
     */
    protected boolean autoReconnect = false;

    /**
     * If autoReconnect is enabled, the initial time to wait between re-connect attempts (in seconds, defaults to 2)
     */
    protected int initialTimeout = 2;

    /**
     * Maximum number of reconnects to attempt if autoReconnect is true, default is 3
     */
    protected int maxReconnects=3;


    /**
     * 	Number of seconds to issue before falling back to master when failed over (when using multi-host failover).
     * 	Whichever condition is met first, 'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' will cause an
     * 	attempt to be made to reconnect to the master. Defaults to 50
     */
    protected int secondsBeforeRetryMaster = 50;

    /**
     * 	Number of queries to issue before falling back to master when failed over (when using multi-host failover).
     * 	Whichever condition is met first, 'queriesBeforeRetryMaster' or 'secondsBeforeRetryMaster' will cause an
     * 	attempt to be made to reconnect to the master. Defaults to 30
     */
    protected int queriesBeforeRetryMaster = 30;

    /**
     * When using loadbalancing, the number of times the driver should cycle through available hosts, attempting to connect.
     * Between cycles, the driver will pause for 250ms if no servers are available.
     */
    protected int retriesAllDown = 120;


    /**
     * When in multiple hosts, after this time in second without used, verification that the connections havn't been lost.
     * When 0, no verification will be done. Defaults to 120
     */
    protected int validConnectionTimeout = 120;

    /**
     * time in second a server is blacklisted after a connection failure.  default to 60s
     */
    protected int loadBalanceBlacklistTimeout = 60;


    /* =========================== Failover variables ========================================= */
    private long masterHostFailTimestamp = 0;
    private long secondaryHostFailTimestamp = 0;
    protected int currentConnectionAttempts = 0;

    protected AtomicBoolean currentReadOnlyAsked=new AtomicBoolean();

    private AtomicBoolean masterHostFail = new AtomicBoolean();
    private AtomicBoolean secondaryHostFail = new AtomicBoolean();
    protected AtomicBoolean isLooping = new AtomicBoolean();
    protected ScheduledFuture scheduledFailover = null;
    protected Protocol currentProtocol = null;
    protected int queriesSinceFailover=0;
    protected long lastRetry = 0;
    protected FailoverProxy proxy;

    /**
     * list the recent failedConnection
     */
    protected static Map<HostAddress, Long> blacklist = new HashMap();

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }
    public FailoverProxy getProxy() { return  this.proxy; }

    public Map<HostAddress, Long> getBlacklist() {
        return blacklist;
    }


    /**
     * parse High availability options.
     *
     */
    protected void parseHAOptions() {
        String s = jdbcUrl.getProperties().getProperty("autoReconnect");
        if (s != null && s.equals("true")) autoReconnect = true;

        s = jdbcUrl.getProperties().getProperty("maxReconnects");
        if (s != null) maxReconnects = Integer.parseInt(s);

        s = jdbcUrl.getProperties().getProperty("queriesBeforeRetryMaster");
        if (s != null) queriesBeforeRetryMaster = Integer.parseInt(s);

        s = jdbcUrl.getProperties().getProperty("secondsBeforeRetryMaster");
        if (s != null) secondsBeforeRetryMaster = Integer.parseInt(s);

        s = jdbcUrl.getProperties().getProperty("retriesAllDown");
        if (s != null) retriesAllDown = Integer.parseInt(s);

        s = jdbcUrl.getProperties().getProperty("validConnectionTimeout");
        if (s != null) validConnectionTimeout = Integer.parseInt(s);

        s = jdbcUrl.getProperties().getProperty("loadBalanceBlacklistTimeout");
        if (s != null) loadBalanceBlacklistTimeout = Integer.parseInt(s);

    }


    public HandleErrorResult handleFailover(Method method, Object[] args, boolean isQuery) throws Throwable {
        if (currentProtocol.mustBeMasterConnection()) {

            if (masterHostFail.compareAndSet(false, true)) {
                log.fine("Handle new Primary failover");
                masterHostFailTimestamp = System.currentTimeMillis();
                currentConnectionAttempts = 0;
                addToBlacklist(currentProtocol.getHostAddress());
            }
            if (isQuery)queriesSinceFailover++;
            return primaryFail(method, args);
        } else {
            if (secondaryHostFail.compareAndSet(false, true)) {
                log.fine("Handle new Secondary failover");
                secondaryHostFailTimestamp = System.currentTimeMillis();
                currentConnectionAttempts = 0;
                addToBlacklist(currentProtocol.getHostAddress());
            }
            return secondaryFail(method, args);
        }
    }

    /**
     * After a failover, put the hostAddress in a static list so the other connection will not take this host in account for a time
     * @param hostAddress the HostAddress to add to blacklist
     */
    public void addToBlacklist(HostAddress hostAddress) {
        if (hostAddress != null) {
            log.fine("host " + hostAddress+" added to blacklist");
            blacklist.put(hostAddress, System.currentTimeMillis());
        }
    }

    /**
     * Permit to remove Host to blacklist after loadBalanceBlacklistTimeout seconds
     */
    public void resetOldsBlackListHosts() {
        long currentTime = System.currentTimeMillis();
        Set<HostAddress> currentBlackListkeys = new HashSet<HostAddress>(blacklist.keySet());
        for (HostAddress blackListHost : currentBlackListkeys) {
            if (blacklist.get(blackListHost) < currentTime - loadBalanceBlacklistTimeout * 1000) {
                log.fine("host " + blackListHost+" remove of blacklist");
                blacklist.remove(blackListHost);
            }
        }
    }


    protected void resetMasterFailoverData()  {
        currentConnectionAttempts = 0;
        if (masterHostFail.compareAndSet(true, false)) {
            masterHostFailTimestamp = 0;
        }
        lastRetry = 0;
        queriesSinceFailover = 0;
    }

    protected void resetSecondaryFailoverData() {
        currentConnectionAttempts = 0;
        if (secondaryHostFail.compareAndSet(true, false)) {
            secondaryHostFailTimestamp = 0;
        }
        lastRetry = 0;
    }

    /**
     * private class to permit a timer reconnection loop
     */
    protected class FailLoop implements Runnable {
        FailoverListener listener;
        public FailLoop(FailoverListener listener) {
            log.finest("launched FailLoop");
            this.listener = listener;
        }

        public void run() {
            synchronized (listener) {
                log.fine("failLoop , has a failover : "+(isMasterHostFail() || isSecondaryHostFail()));
                if (isMasterHostFail() || isSecondaryHostFail()) {
                    log.fine("failLoop , listener.shouldReconnect() : "+listener.shouldReconnect());
                    if (listener.shouldReconnect()) {
                        try {
                            listener.reconnectFailedConnection();
                            //reconnection done !
                            stopFailover();
                        } catch (Exception e) {
                            log.finest("FailLoop search connection failed");
                            //do nothing
                        }
                    } else {
                        if (currentConnectionAttempts > retriesAllDown) {
                            log.fine("stopping failover after too many attemps ("+currentConnectionAttempts+")");
                            stopFailover();
                        }
                    }
                } else {
                    stopFailover();
                }
            }
        }
    }

    protected void setSessionReadOnly(boolean readOnly) throws QueryException {
        if (this.currentProtocol.versionGreaterOrEqual(10, 0, 0)) {
            this.currentProtocol.executeQuery(new MySQLQuery("SET SESSION TRANSACTION "+(readOnly?"READ ONLY":"READ WRITE")));
        }
    }

    protected void stopFailover() {
        if (isLooping.compareAndSet(true, false)) {
            log.fine("stopping failover");
            if (scheduledFailover!=null)scheduledFailover.cancel(false);
        }
    }

    /**
     * launch the scheduler loop every 250 milliseconds, to reconnect a failed connection.
     * Will verify if there is an existing scheduler
     * @param now now will launch the loop immediatly, 250ms after if false
     */
    protected void launchFailLoopIfNotlaunched(boolean now) {
        if (isLooping.compareAndSet(false, true)) {
            scheduledFailover = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new FailLoop(this), now ? 0 : 250, 250, TimeUnit.MILLISECONDS);
        }
    }

    public Protocol getCurrentProtocol() {
        return currentProtocol;
    }

    public long getMasterHostFailTimestamp() {
        return masterHostFailTimestamp;
    }

    public long getSecondaryHostFailTimestamp() {
        return secondaryHostFailTimestamp;
    }

    public boolean setMasterHostFail() {
        if (masterHostFail.compareAndSet(false, true)) {
            masterHostFailTimestamp = System.currentTimeMillis();
            currentConnectionAttempts = 0;
            return true;
        }
        return false;
    }

    public boolean setSecondaryHostFail() {
        if (secondaryHostFail.compareAndSet(false, true)) {
            secondaryHostFailTimestamp = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public boolean isMasterHostFail() {
        return masterHostFail.get();
    }

    public boolean isSecondaryHostFail() {
        return secondaryHostFail.get();
    }
    public void additionnalQuerySinceFailover() {
        queriesSinceFailover++;
    }




    /**
     * After a failover that has bean done, relaunche the operation that was in progress.
     * In case of special operation that crash serveur, doesn't relaunched it;
     * @param method the methode accessed
     * @param args the parameters
     * @return An object that indicate the result or that the exception as to be thrown
     * @throws IllegalAccessException if the initial call is not permit
     * @throws InvocationTargetException if there is any error relaunching initial method
     */
    public HandleErrorResult relaunchOperation(Method method, Object[] args) throws IllegalAccessException, InvocationTargetException{
        HandleErrorResult handleErrorResult = new HandleErrorResult();
        if (method != null) {
            if ("executeQuery".equals(method.getName())) {
                String query = ((Query)args[0]).getQuery().toUpperCase();
                if (!query.equals("ALTER SYSTEM CRASH")
                        && !query.startsWith("KILL")) {
                    handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                    handleErrorResult.mustThrowError = false;
                }
            } else {
                handleErrorResult.resultObject = method.invoke(currentProtocol, args);
                handleErrorResult.mustThrowError = false;
            }
        }
        return handleErrorResult;
    }

    public Object invoke(Method method, Object[] args) throws Throwable {
        return  method.invoke(currentProtocol, args);
    }


    /**
     * when switching between 2 connections, report existing connection parameter to the new used connection
     * @param from used connection
     * @param to will-be-current connection
     * @throws QueryException
     * @throws SQLException
     */
    public void syncConnection(Protocol from, Protocol to) throws QueryException {
        if (from != null) {
            to.setMaxAllowedPacket(from.getMaxAllowedPacket());
            to.setMaxRows(from.getMaxRows());
            to.setInternalMaxRows(from.getMaxRows());
            if (from.getTransactionIsolationLevel() != 0) {
                to.setTransactionIsolation(from.getTransactionIsolationLevel());
            }
            if (from.getDatabase() != null && !"".equals(from.getDatabase())) {
                to.selectDB(from.getDatabase());
            }
            if (from.getAutocommit() != to.getAutocommit()) {
                to.executeQuery(new MySQLQuery("set autocommit=" + (from.getAutocommit() ? "1" : "0")));
            }
        }
    }
    public JDBCUrl getJdbcUrl() {
        return jdbcUrl;
    }

    public abstract void initializeConnection() throws QueryException;

    public abstract void preExecute() throws SQLException;

    public abstract void preClose() throws SQLException;

    public abstract boolean shouldReconnect() ;

    public abstract void reconnectFailedConnection() throws QueryException ;

    public abstract void switchReadOnlyConnection(Boolean readonly) throws QueryException;

    public abstract HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable ;

    public abstract HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable;
}
