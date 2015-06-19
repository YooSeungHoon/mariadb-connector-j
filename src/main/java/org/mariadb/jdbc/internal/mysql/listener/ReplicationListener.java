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

package org.mariadb.jdbc.internal.mysql.listener;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.mysql.*;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * this class handle the operation when multiple hosts.
 */
public class ReplicationListener extends BaseFailoverListener implements FailoverListener {
    private final static Logger log = Logger.getLogger(ReplicationListener.class.getName());

    protected ReplicationProtocol masterProtocol;
    protected ReplicationProtocol secondaryProtocol;
    protected long lastQueryTime = 0;
    protected ScheduledFuture scheduledPing = null;

    public ReplicationListener() {
        masterProtocol = null;
        secondaryProtocol = null;
    }

    public void initializeConnection(Protocol protocol) throws QueryException, SQLException {
        this.masterProtocol = (ReplicationProtocol)protocol;
        this.currentProtocol = this.masterProtocol;
        parseHAOptions(protocol);
        //if (validConnectionTimeout != 0) scheduledPing = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new PingLoop(this), 1, 1, TimeUnit.SECONDS);

        if (validConnectionTimeout != 0) scheduledPing = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new PingLoop(this), validConnectionTimeout, validConnectionTimeout, TimeUnit.SECONDS);
        try {
            reconnectFailedConnection(true, true, true);
        } catch (QueryException e) {
            checkInitialConnection();
            throw e;
        } catch (SQLException e) {
            checkInitialConnection();
            throw e;
        }
    }

    protected void checkInitialConnection() {
        if (this.masterProtocol != null && !this.masterProtocol.isConnected()) {
            setMasterHostFail();
        }
        if (this.secondaryProtocol != null && !this.secondaryProtocol.isConnected()) {
            setSecondaryHostFail();
        }
        launchFailLoopIfNotlaunched(false);
    }

    public void preClose()  throws SQLException {
        if (scheduledPing != null) scheduledPing.cancel(true);
        if (scheduledFailover!=null)scheduledFailover.cancel(true);
        if (!this.masterProtocol.isClosed()) this.masterProtocol.close();
        if (!this.secondaryProtocol.isClosed()) this.secondaryProtocol.close();
    }

    @Override
    public void preExecute() throws SQLException {
        if (isMasterHostFail())queriesSinceFailover++;

        if (shouldReconnect()) {
            launchAsyncSearchLoopConnection();
        } else if (validConnectionTimeout != 0) {
            lastQueryTime = System.currentTimeMillis();
            scheduledPing.cancel(true);
            scheduledPing = Executors.newSingleThreadScheduledExecutor().schedule(new PingLoop(this), validConnectionTimeout, TimeUnit.SECONDS);
        }
    }



    /**
     * verify the different case when the connector must reconnect to host.
     * the autoreconnect parameter for multihost is only used after an error to try to reconnect and relaunched the operation silently.
     * So he doesn't appear here.
     * @return true if should reconnect.
     */
    public boolean shouldReconnect() {
        if (isMasterHostFail() || isSecondaryHostFail()) {
            if (currentProtocol.inTransaction()) return false;
            if (currentConnectionAttempts > retriesAllDown) return false;
            long now = System.currentTimeMillis();

            if (isMasterHostFail()) {
                if (queriesSinceFailover >= queriesBeforeRetryMaster) return true;
                if ((now - getMasterHostFailTimestamp()) >= secondsBeforeRetryMaster * 1000) return true;
            }

            if (isSecondaryHostFail()) {
                if ((now - getSecondaryHostFailTimestamp()) >= secondsBeforeRetryMaster * 1000) return true;
            }
        }
        return false;
    }

    /**
     * Asynchronous Loop to replace failed connections with valid ones.
     *
     */
    public void launchAsyncSearchLoopConnection() {
        final ReplicationListener hostListener = ReplicationListener.this;
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
            try {
                hostListener.reconnectFailedConnection();
            } catch (Exception e) { }
            }
        });
    }

    /**
     * Loop to replace failed connections with valid ones.
     * @throws QueryException if there is any error during reconnection
     * @throws SQLException sqlException
     */
    public void reconnectFailedConnection() throws QueryException, SQLException {
        currentConnectionAttempts++;
        lastRetry = System.currentTimeMillis();
        if (currentConnectionAttempts >= retriesAllDown) throw new QueryException("Too many reconnection attempts ("+retriesAllDown+")");
        reconnectFailedConnection(isMasterHostFail(), isSecondaryHostFail(), false);
    }

    public synchronized void reconnectFailedConnection(boolean searchForMaster, boolean searchForSecondary, boolean initialConnection) throws QueryException, SQLException {

        resetOldsBlackListHosts();
        List<HostAddress> loopAddress = new LinkedList(this.masterProtocol.getJdbcUrl().getHostAddresses());
        loopAddress.removeAll(blacklist.keySet());

        if (((searchForMaster && isMasterHostFail())|| (searchForSecondary && isSecondaryHostFail())) || initialConnection) {
            this.masterProtocol.loop(this, loopAddress, blacklist, new SearchFilter(searchForMaster, searchForSecondary));
        }
    }

    /**
     * method called when a new Master connection is found after a fallback
     * @param newMasterProtocol the new active connection
     */
    public void foundActiveMaster(Protocol newMasterProtocol) {
        this.masterProtocol = (ReplicationProtocol) newMasterProtocol;
        if (!currentReadOnlyAsked.get()) {
            //actually on a secondary read-only because master was unknown.
            //So select master as currentConnection
            try {
                syncConnection(currentProtocol, this.masterProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching current connection to master connection");
            currentProtocol = this.masterProtocol;
        }
        if (log.isLoggable(Level.INFO)) {
            if (isMasterHostFail()) {
                log.info("new primary node [" + newMasterProtocol.getHostAddress().toString() + "] connection established after " + (System.currentTimeMillis() - getMasterHostFailTimestamp()));
            } else log.info("new primary node [" + newMasterProtocol.getHostAddress().toString() + "] connection established");
        }
        resetMasterFailoverData();

    }


    /**
     * method called when a new secondary connection is found after a fallback
     * @param newSecondaryProtocol the new active connection
     */
    public void foundActiveSecondary(ReplicationProtocol newSecondaryProtocol) {
        log.fine("found active secondary connection");
        this.secondaryProtocol = newSecondaryProtocol;

        //if asked to be on read only connection, switching to this new connection
        if (currentReadOnlyAsked.get() || (!currentReadOnlyAsked.get() && isMasterHostFail())) {
            try {
                syncConnection(currentProtocol, this.secondaryProtocol);
            } catch (Exception e) {
                log.fine("Some error append during connection parameter synchronisation : " + e.getMessage());
            }
            log.finest("switching current connection to secondary connection");
            currentProtocol = this.secondaryProtocol;
        }

        if (log.isLoggable(Level.INFO)) {
            if (isSecondaryHostFail()) {
                log.info("new active secondary node [" + newSecondaryProtocol.getHostAddress().toString() + "] connection established after " + (System.currentTimeMillis() - getSecondaryHostFailTimestamp()));
            } else log.info("new active secondary node [" + newSecondaryProtocol.getHostAddress().toString() + "] connection established");

        }
        resetSecondaryFailoverData();
    }

    /**
     * switch to a read-only(secondary) or read and write connection(master)
     * @param mustBeReadOnly the read-only status asked
     * @throws SQLException if operation hasn't change protocol
     */
    @Override
    public void switchReadOnlyConnection(Boolean mustBeReadOnly) throws SQLException {
        try {
            log.finest("switching to mustBeReadOnly = " + mustBeReadOnly + " mode");

            if (mustBeReadOnly != currentReadOnlyAsked.get() && currentProtocol.inTransaction()) {
                throw new QueryException("Trying to set to read-only mode during a transaction");
            }
            if (currentReadOnlyAsked.compareAndSet(!mustBeReadOnly, mustBeReadOnly)) {
                if (currentReadOnlyAsked.get()) {
                    if (currentProtocol.isMasterConnection()) {
                        //must change to replica connection
                        if (!isSecondaryHostFail()) {
                            synchronized (this) {
                                log.finest("switching to secondary connection");
                                syncConnection(this.masterProtocol, this.secondaryProtocol);
                                currentProtocol = this.secondaryProtocol;
                                setSessionReadOnly(true);
                                log.finest("current connection is now secondary");
                            }
                        }
                    }
                } else {
                    if (!currentProtocol.isMasterConnection()) {
                        //must change to master connection
                        if (!isMasterHostFail()) {
                            synchronized (this) {
                                log.finest("switching to master connection");
                                syncConnection(this.secondaryProtocol, this.masterProtocol);
                                currentProtocol = this.masterProtocol;
                                log.finest("current connection is now master");
                            }
                        } else {
                            if (autoReconnect) {
                                try {
                                    reconnectFailedConnection();
                                    //connection established, no need to send Exception !
                                    return;
                                } catch (Exception e) { }
                            }
                            launchFailLoopIfNotlaunched(false);
                            throw new QueryException("No primary host is actually connected");
                        }
                    }
                }
            }
        } catch (QueryException e) {
            SQLExceptionMapper.throwException(e, null, null);
        }

    }

    /**
     * when switching between 2 connections, report existing connection parameter to the new used connection
     * @param from used connection
     * @param to will-be-current connection
     * @throws QueryException
     * @throws SQLException
     */
    protected void syncConnection(Protocol from, Protocol to) throws QueryException, SQLException {
        to.setMaxAllowedPacket(from.getMaxAllowedPacket());
        to.setMaxRows(from.getMaxRows());
        to.setInternalMaxRows(from.getMaxRows());
        if (from.getTransactionIsolationLevel() != 0) {
            to.setTransactionIsolation(from.getTransactionIsolationLevel());
        }
        try {
            if (from.getDatabase() != null && !"".equals(from.getDatabase())) {
                    to.selectDB(from.getDatabase());
            }
            if (from.getAutocommit() != to.getAutocommit()) {
                to.executeQuery(new MySQLQuery("set autocommit=" + (from.getAutocommit()?"1":"0")));
            }
        } catch (QueryException e) {
            e.printStackTrace();
        }
    }

    /**
     * to handle the newly detected failover on the master connection
     * @param method the initial called method
     * @param args the initial args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable
     */
    public HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        log.warning("SQL Primary node [" + this.masterProtocol.getHostAddress().toString() + "] connection fail ");

        //try to reconnect automatically only time before looping
        try {
            if(this.masterProtocol != null && this.masterProtocol.ping()) {
                log.info("SQL Primary node [" + this.masterProtocol.getHostAddress().toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        } catch (Exception e) {
            if (setMasterHostFail()) addToBlacklist(this.masterProtocol.getHostAddress());
        }

        if (autoReconnect && !this.masterProtocol.inTransaction()) {
            try {
                reconnectFailedConnection();
                log.finest("SQL Primary node [" + this.masterProtocol.getHostAddress().toString() + "] connection re-established");

                //now that we are reconnect, relaunched result if the result was not crashing the node
                return relaunchOperation(method, args);
            } catch (Exception e) { }
        }

        //in multiHost, switch to secondary if active, even if in a current transaction -> will throw an exception
        if (!isSecondaryHostFail()) {
            try {
                if(this.secondaryProtocol != null && this.secondaryProtocol.ping()) {
                    log.finest("switching to secondary connection");
                    syncConnection(this.masterProtocol, this.secondaryProtocol);
                    currentProtocol = this.secondaryProtocol;
                    launchFailLoopIfNotlaunched(true);
                    try {
                        return relaunchOperation(method, args);
                    } catch (Exception e) {
                        //if a problem since ping, just launched the first exception
                    }
                } else log.finest("ping failed on secondary");
            } catch (Exception e) {
                if (setSecondaryHostFail()) addToBlacklist(this.secondaryProtocol.getHostAddress());
                log.log(Level.FINEST, "ping on secondary failed");
            }
        } else log.finest("secondary is already down");

        log.finest("no secondary failover active");
        launchFailLoopIfNotlaunched(true);
        return new HandleErrorResult();
    }

    /**
     * to handle the newly detected failover on the secondary connection
     * @param method the initial called method
     * @param args the initial args
     * @return an object to indicate if the previous Exception must be thrown, or the object resulting if a failover worked
     * @throws Throwable
     */
    public HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {
        try {
            if(this.secondaryProtocol != null && this.secondaryProtocol.ping()) {
                log.info("SQL Secondary node [" + this.secondaryProtocol.getHostAddress().toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        } catch (Exception e) {
            log.finest("ping fail on secondary");
            if (setSecondaryHostFail()) addToBlacklist(this.secondaryProtocol.getHostAddress());
        }
        log.finest("isMasterHostFail() " + isMasterHostFail());

        if (!isMasterHostFail()) {
            try {
                if (this.masterProtocol !=null) {
                    this.masterProtocol.ping(); //check that master is on before switching to him
                    log.finest("switching to master connection");
                    syncConnection(this.secondaryProtocol, this.masterProtocol);
                    currentProtocol = this.masterProtocol;

                    launchFailLoopIfNotlaunched(true); //launch reconnection loop
                    return relaunchOperation(method, args); //now that we are on master, relaunched result if the result was not crashing the master
                }
            } catch (Exception e) {
                log.finest("ping fail on master");
                if (setMasterHostFail()) addToBlacklist(this.masterProtocol.getHostAddress());
            }
        }

        if (autoReconnect) {
            try {
                reconnectFailedConnection();
                log.finest("SQL secondary node [" + this.secondaryProtocol.getHostAddress().toString() + "] connection re-established");
                return relaunchOperation(method, args); //now that we are reconnect, relaunched result if the result was not crashing the node
            } catch (Exception ee) {
                //in case master is down and another slave has been found
                if (!isSecondaryHostFail()) {
                    return relaunchOperation(method, args);
                }
                launchFailLoopIfNotlaunched(false);
                return new HandleErrorResult();
            }
        }

        launchFailLoopIfNotlaunched(true);
        return new HandleErrorResult();
    }





    /**
     * private class to chech of currents connections are still ok.
     */
    protected class PingLoop implements Runnable {
        ReplicationListener listener;
        public PingLoop(ReplicationListener listener) {
            this.listener = listener;
        }

        public void run() {
            //if (lastQueryTime + validConnectionTimeout < System.currentTimeMillis()) {
            synchronized (listener) {

                log.finest("PingLoop run");
                if (!isMasterHostFail()) {
                    log.finest("PingLoop run, master not see failed");
                    boolean masterFail = false;
                    try {
                        if (masterProtocol.ping()) {
                            if (!masterProtocol.checkIfMaster()) {
                                //the connection that was master isn't now
                                masterFail = true;
                            }
                        }
                    } catch (QueryException e) {
                        log.finest("PingLoop ping to master error");
                        masterFail = true;
                    }

                    if (masterFail) {
                        log.finest("PingLoop master failed -> will loop to found it");
                        if (setMasterHostFail()) {
                            try {
                                listener.primaryFail(null, null);
                            } catch (Throwable t) {
                                //do nothing
                            }
                        }
                    }
                }
            }
        }
    }
}
