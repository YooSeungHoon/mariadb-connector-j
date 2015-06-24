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

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.MySQLQuery;
import org.mariadb.jdbc.internal.common.queryresults.SelectQueryResult;
import org.mariadb.jdbc.internal.mysql.listener.AuroraListener;
import org.mariadb.jdbc.internal.mysql.listener.SearchFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AuroraMultiNodesProtocol extends ReplicationProtocol {
    private final static Logger log = Logger.getLogger(AuroraMultiNodesProtocol.class.getName());

    public AuroraMultiNodesProtocol(final JDBCUrl url) {
        super(url);
    }

    @Override
    public boolean isMasterConnection() {
        return this.masterConnection;
    }
    /**
     * Aurora best way to check if a node is a master : is not in read-only mode
     * @return
     */
    @Override
    public boolean checkIfMaster() throws QueryException {
        try {

            SelectQueryResult queryResult = (SelectQueryResult) executeQuery(new MySQLQuery("show global variables like 'innodb_read_only'"));
            if (queryResult != null) {
                queryResult.next();
                this.masterConnection = "OFF".equals(queryResult.getValueObject(1).getString());
            } else {
                this.masterConnection = false;
            }
            this.readOnly = !this.masterConnection;
            return this.masterConnection;

        } catch(IOException ioe) {
            throw new QueryException("could not check the 'innodb_read_only' variable status on " + this.getHostAddress() +
                    " : " + ioe.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), ioe);
        }
    }



    public static void searchProbableMaster(AuroraListener listener, HostAddress probableMaster, Map<HostAddress, Long> blacklist, SearchFilter searchFilter) throws QueryException {
        if (log.isLoggable(Level.FINE)) {
            log.fine("searching for master:"+ searchFilter.isSearchForMaster()+ " replica:"+ searchFilter.isSearchForSlave()+ " address:"+probableMaster+" blacklist:"+blacklist.keySet());
        }
        AuroraMultiNodesProtocol protocol = getNewProtocol(listener.getProxy(), listener.getJdbcUrl());
        try {

            protocol.setHostAddress(probableMaster);
            if (log.isLoggable(Level.FINE)) log.fine("trying to connect to " + protocol.getHostAddress());
            protocol.connect();
            if (log.isLoggable(Level.FINE)) log.fine("connected to " + protocol.getHostAddress());

            if (searchFilter.isSearchForMaster() && protocol.isMasterConnection()) {
                searchFilter.setSearchForMaster(false);
                protocol.setMustBeMasterConnection(true);
                listener.foundActiveMaster(protocol);
            } else if (searchFilter.isSearchForSlave() &&  !protocol.isMasterConnection()) {
                searchFilter.setSearchForSlave(false);
                protocol.setMustBeMasterConnection(false);
                listener.foundActiveSecondary(protocol);
            }
        } catch (QueryException e ) {
            blacklist.put(protocol.getHostAddress(), System.currentTimeMillis());
            if (log.isLoggable(Level.FINE)) log.fine("Could not connect to " + protocol.currentHost + " searching for master : " + searchFilter.isSearchForMaster() + " for replica :" + searchFilter.isSearchForSlave() + " error:" + e.getMessage());
        }
    }

    /**
     * loop until found the failed connection.
     * search order :
     * - in the host without the failed ones
     * - the failed host if not found
     * @param listener
     * @param addresses
     * @param blacklist
     * @param searchFilter
     * @throws QueryException
     */
    public static void loop(AuroraListener listener, List<HostAddress> addresses, Map<HostAddress, Long> blacklist, SearchFilter searchFilter) throws QueryException {
        if (log.isLoggable(Level.FINE)) {
            log.fine("searching for master:"+ searchFilter.isSearchForMaster()+ " replica:"+ searchFilter.isSearchForSlave()+ " addresses:"+addresses+" blacklist:"+blacklist.keySet());
        }
        List initialBlackList = new ArrayList(blacklist.keySet());
        searchRandomProtocol(getNewProtocol(listener.getProxy(), listener.getJdbcUrl()), listener, addresses, blacklist, searchFilter);

        if (searchFilter.isSearchForMaster() || searchFilter.isSearchForSlave()) {
            searchRandomProtocol(getNewProtocol(listener.getProxy(), listener.getJdbcUrl()), listener, initialBlackList, null, searchFilter);
        }
        if (searchFilter.isSearchForMaster() || searchFilter.isSearchForSlave()) {
            if (searchFilter.isSearchForMaster()) throw new QueryException("No active connection found for master");
            else throw new QueryException("No active connection found for replica");
        }
    }

    private static void searchRandomProtocol(AuroraMultiNodesProtocol protocol, AuroraListener listener, final List<HostAddress> addresses, Map<HostAddress, Long> blacklist, SearchFilter searchFilter) throws QueryException {
        List<HostAddress> searchAddresses = new ArrayList<HostAddress>(addresses);
        if (blacklist!=null) searchAddresses.removeAll(blacklist.keySet());

        Random rand = new Random();
        while (!addresses.isEmpty()) {
            try {
                int index = rand.nextInt(searchAddresses.size());
                protocol.setHostAddress(searchAddresses.get(index));
                searchAddresses.remove(index);

                if (log.isLoggable(Level.FINE)) log.fine("trying to connect to " + protocol.getHostAddress());
                protocol.connect();
                if (log.isLoggable(Level.FINE)) log.fine("connected to " + protocol.getHostAddress());

                if (searchFilter.isSearchForMaster() && protocol.isMasterConnection()) {
                    searchFilter.setSearchForMaster(false);
                    protocol.setMustBeMasterConnection(true);
                    listener.foundActiveMaster(protocol);
                    if (!searchFilter.isSearchForSlave()) return;
                    else protocol = getNewProtocol(listener.getProxy(), listener.getJdbcUrl());
                } else if (searchFilter.isSearchForSlave() &&  !protocol.isMasterConnection()) {
                    searchFilter.setSearchForSlave(false);
                    protocol.setMustBeMasterConnection(false);
                    listener.foundActiveSecondary(protocol);
                    if (!searchFilter.isSearchForMaster()) return;
                    else {
                        listener.searchByStartName(protocol, addresses);
                        protocol = getNewProtocol(listener.getProxy(), listener.getJdbcUrl());
                    }
                }
            } catch (QueryException e ) {
                if (blacklist!=null)blacklist.put(protocol.getHostAddress(), System.currentTimeMillis());
                if (log.isLoggable(Level.FINE)) log.fine("Could not connect to " + protocol.currentHost + " searching for master : " + searchFilter.isSearchForMaster() + " for replica :" + searchFilter.isSearchForSlave() + " error:" + e.getMessage());
            }
            if (!searchFilter.isSearchForMaster() && !searchFilter.isSearchForSlave()) return;
        }
    }

    public static AuroraMultiNodesProtocol getNewProtocol(FailoverProxy proxy, JDBCUrl jdbcUrl) {
        AuroraMultiNodesProtocol newProtocol = new AuroraMultiNodesProtocol(jdbcUrl);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }


}
