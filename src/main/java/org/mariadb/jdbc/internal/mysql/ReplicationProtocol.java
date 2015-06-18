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
import org.mariadb.jdbc.internal.mysql.listener.Listener;
import org.mariadb.jdbc.internal.mysql.listener.ReplicationListener;
import org.mariadb.jdbc.internal.mysql.listener.SearchFilter;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

public class ReplicationProtocol extends MySQLProtocol {

    boolean masterConnection = false;
    boolean mustBeMasterConnection = false;

    public ReplicationProtocol(final JDBCUrl url) {
        super(url);
    }

    @Override
    public void connect() throws QueryException {
        if (!isClosed()) {
            close();
        }

        // There could be several addresses given in the URL spec, try all of them, and throw exception if all hosts
        // fail.
        List<HostAddress> addrs = this.jdbcUrl.getHostAddresses();
        for(int i = 0; i < addrs.size(); i++) {
            currentHost = addrs.get(i);
            try {
                connect(currentHost.host, currentHost.port);
                return;
            } catch (IOException e) {
                if (i == addrs.size() - 1) {
                    throw new QueryException("Could not connect to " + HostAddress.toString(addrs) +
                            " : " + e.getMessage(),  -1,  SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }
            }
        }
    }



    public void connectMaster(ReplicationListener listener) throws QueryException  {
        //Master is considered the firstOne
        HostAddress host = jdbcUrl.getHostAddresses().get(0);
        try {
            currentHost = host;
            log.fine("trying to connect master " + currentHost);
            connect(currentHost.host, currentHost.port);
            listener.foundActiveMaster(this);
            setMustBeMasterConnection(true);
        } catch (IOException e) {
            throw new QueryException("Could not connect to " + host + " : " + e.getMessage(), -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
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
    @Override
    public void loop(Listener listener, List<HostAddress> addresses, Map<HostAddress, Long> blacklist, SearchFilter searchFilter) throws QueryException {
        if (log.isLoggable(Level.FINE)) {
            log.fine("searching for master:"+ searchFilter.isSearchForMaster()+ " replica:"+ searchFilter.isSearchForSlave()+ " address:"+addresses+" blacklist:"+blacklist.keySet());
        }
        ReplicationProtocol protocol = getNewProtocol();
        List initialBlackList = new ArrayList(blacklist.keySet());
        searchRandomProtocol(protocol, (ReplicationListener) listener, addresses, blacklist, searchFilter);

        if (searchFilter.isSearchForMaster() || searchFilter.isSearchForSlave()) {
            searchRandomProtocol(protocol, (ReplicationListener)listener, initialBlackList, null, searchFilter);
        }
        if (searchFilter.isSearchForMaster() || searchFilter.isSearchForSlave()) {
            if (searchFilter.isSearchForMaster()) throw new QueryException("No active connection found for master");
            else throw new QueryException("No active connection found for replica");
        }
    }

    private void searchRandomProtocol(ReplicationProtocol protocol, ReplicationListener listener, final List<HostAddress> addresses, Map<HostAddress, Long> blacklist,  SearchFilter searchFilter) throws QueryException {
        List<HostAddress> searchAddresses = new ArrayList<HostAddress>(addresses);
        if (blacklist!=null) searchAddresses.removeAll(blacklist.keySet());

        Random rand = new Random();

        while (!searchAddresses.isEmpty()) {

            int index = rand.nextInt(searchAddresses.size());
            protocol.setHostAddress(searchAddresses.get(index));
            searchAddresses.remove(index);
            try {

                if ((protocol.isMasterConnection() && searchFilter.isSearchForMaster()) || (!protocol.isMasterConnection() && searchFilter.isSearchForSlave())) {
                    protocol.connect(protocol.getHostAddress().host, protocol.getHostAddress().port);

                    if (searchFilter.isSearchForMaster() && protocol.isMasterConnection()) {

                        searchFilter.setSearchForMaster(false);
                        protocol.setMustBeMasterConnection(true);
                        listener.foundActiveMaster(protocol);

                        if (!searchFilter.isSearchForSlave()) return;
                        else protocol = getNewProtocol();

                    } else if (searchFilter.isSearchForSlave() && !protocol.isMasterConnection()) {

                        searchFilter.setSearchForSlave(false);
                        protocol.setMustBeMasterConnection(false);
                        listener.foundActiveSecondary(protocol);

                        if (!searchFilter.isSearchForMaster()) return;
                        else protocol = getNewProtocol();
                    }
                }

            } catch (QueryException e ) {
                if (blacklist!=null)blacklist.put(protocol.getHostAddress(), System.currentTimeMillis());
                log.fine("Could not connect to " + protocol.getHostAddress() + " searching for master : " + searchFilter.isSearchForMaster() + " for replica :" + searchFilter.isSearchForSlave() + " error:" + e.getMessage());
            } catch (IOException e ) {
                if (blacklist!=null)blacklist.put(protocol.getHostAddress(), System.currentTimeMillis());
                log.fine("Could not connect to " + protocol.getHostAddress() + " searching for master : " + searchFilter.isSearchForMaster() + " for replica :" + searchFilter.isSearchForSlave() + " error:" + e.getMessage());
            }
            if (!searchFilter.isSearchForMaster() && !searchFilter.isSearchForSlave()) return;
        }
    }

    @Override
    public ReplicationProtocol getNewProtocol() {
        ReplicationProtocol newProtocol = new ReplicationProtocol(this.jdbcUrl);
        newProtocol.setProxy(proxy);
        return newProtocol;
    }

    @Override
    public boolean createDB() {
        if (masterConnection) {
            String alias = jdbcUrl.getProperties().getProperty("createDatabaseIfNotExist");
            return jdbcUrl.getProperties() != null
                    && (jdbcUrl.getProperties().getProperty("createDB", "").equalsIgnoreCase("true")
                    || (alias != null && alias.equalsIgnoreCase("true")));
        }
        return false;
    }

    public boolean mustBeMasterConnection() {
        return mustBeMasterConnection;
    }
    public void setMustBeMasterConnection(boolean mustBeMasterConnection) {
        this.mustBeMasterConnection = mustBeMasterConnection;
    }
}
