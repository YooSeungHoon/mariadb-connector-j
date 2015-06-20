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

import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.mysql.HandleErrorResult;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MasterOnlyFailoverListener extends BaseFailoverListener implements FailoverListener {
    private final static Logger log = Logger.getLogger(MasterOnlyFailoverListener.class.getName());

    public MasterOnlyFailoverListener() { }

    public void initializeConnection(Protocol protocol) throws QueryException {
        this.currentProtocol = protocol;
        parseHAOptions(this.currentProtocol);
        log.fine("launching initial loop");
        this.currentProtocol.loop(this, protocol.getJdbcUrl().getHostAddresses(), blacklist, null);
        log.fine("launching initial loop end");
    }

    public void preExecute() throws SQLException {
        if (isMasterHostFail())queriesSinceFailover++;

    }


    public boolean shouldReconnect() {
        return (isMasterHostFail() && currentConnectionAttempts < maxReconnects);
    }

    public void reconnectFailedConnection() throws QueryException {
        currentConnectionAttempts++;
        log.fine("launching reconnectFailedConnection loop");
        this.currentProtocol.loop(this, this.currentProtocol.getJdbcUrl().getHostAddresses(), blacklist, null);
        log.fine("launching reconnectFailedConnection loop end");

        //if no error, reset failover variables
        resetMasterFailoverData();
    }


    public void switchReadOnlyConnection(Boolean readonly) throws QueryException {
        setSessionReadOnly(readonly);
        this.currentProtocol.setReadonly(readonly);
    }

    public void preClose()  throws SQLException {
        if (scheduledFailover!=null) scheduledFailover.cancel(true);
    }

    public HandleErrorResult primaryFail(Method method, Object[] args) throws Throwable {
        try {
            if(this.currentProtocol != null && this.currentProtocol.ping()) {
                log.info("SQL Primary node [" + this.currentProtocol.getHostAddress().toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        } catch (Exception e) {
            if (setMasterHostFail()) addToBlacklist(this.currentProtocol.getHostAddress());
        }

        if (autoReconnect && shouldReconnect()) {
            if (this.currentProtocol.getJdbcUrl().getHostAddresses().size() == 1) {
                //if not first attempt to connect, wait for initialTimeout
                if (currentConnectionAttempts > 0) {
                    try {
                        Thread.sleep(initialTimeout * 1000);
                    } catch (InterruptedException e) {
                    }
                }
            }

            if (!this.currentProtocol.inTransaction()) {
                //trying to reconnect transparently
                reconnectFailedConnection();
                log.finest("SQL Primary node [" + this.currentProtocol.getHostAddress().toString() + "] connection re-established");
                return relaunchOperation(method, args);
            }
        }

        launchFailLoopIfNotlaunched(true);
        return new HandleErrorResult();
    }


    public HandleErrorResult secondaryFail(Method method, Object[] args) throws Throwable {
        return new HandleErrorResult();
    }

    /**
     * method called when a new Master connection is found after a fallback
     * @param protocol the new active connection
     */
    public void foundActiveMaster(Protocol protocol) throws QueryException {
        syncConnection(this.currentProtocol, protocol);
        if (currentProtocol.getReadonly()) {
            protocol.setReadonly(true);
            currentProtocol = protocol;
            try {
                setSessionReadOnly(true);
            } catch (QueryException e) {
                SQLExceptionMapper.getSQLException("Error setting connection read-only after a failover", e);
            }
        } else currentProtocol = protocol;

        if (log.isLoggable(Level.INFO)) {
            if (isMasterHostFail()) {
                log.info("new primary node [" + currentProtocol.getHostAddress().toString() + "] connection established after " + (System.currentTimeMillis() - getMasterHostFailTimestamp()));
            } else log.info("new primary node [" + currentProtocol.getHostAddress().toString() + "] connection established");
        }

        resetMasterFailoverData();

    }
}
