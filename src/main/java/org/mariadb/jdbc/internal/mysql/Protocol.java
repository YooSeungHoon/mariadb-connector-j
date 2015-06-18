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
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.Query;
import org.mariadb.jdbc.internal.common.queryresults.QueryResult;
import org.mariadb.jdbc.internal.common.queryresults.StreamingSelectResult;
import org.mariadb.jdbc.internal.mysql.listener.Listener;
import org.mariadb.jdbc.internal.mysql.listener.SearchFilter;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
public interface Protocol {
    MySQLProtocol.PrepareResult prepare(String sql) throws QueryException;

    void closePreparedStatement(int statementId) throws QueryException;

    boolean getAutocommit();

    boolean noBackslashEscapes();

    void connect() throws QueryException;
    void connect(String host, int port) throws QueryException, IOException;
    void loop(Listener listener, List<HostAddress> addresses, Map<HostAddress, Long> connectionsBlacklist, SearchFilter searchFilter) throws QueryException;

    JDBCUrl getJdbcUrl();
    boolean inTransaction();
    void setProxy(FailoverProxy proxy);
    FailoverProxy getProxy();
    void setHostAddress(HostAddress hostAddress);

    Properties getInfo();

    boolean  hasMoreResults();

    void close();

    boolean isClosed();

    void selectDB(String database) throws QueryException;

    String getServerVersion();
    Protocol getNewProtocol();
    void setReadonly(boolean readOnly);
    boolean isConnected();
    boolean getReadonly();
    boolean isMasterConnection();
    boolean mustBeMasterConnection();
    boolean isHighAvailability();
    HostAddress getHostAddress();
    String getHost();

    int getPort();

    String getDatabase();

    String getUsername();

    String getPassword();

    boolean ping() throws QueryException;

    QueryResult executeQuery(Query dQuery)  throws QueryException;
    QueryResult executeQuery(final List<Query> dQueries, boolean streaming, boolean isRewritable, int rewriteOffset) throws QueryException;
    QueryResult getResult(List<Query> dQuery, boolean streaming) throws QueryException;

    QueryResult executeQuery(Query dQuery, boolean streaming) throws QueryException;

    String getServerVariable(String variable) throws QueryException;

    void cancelCurrentQuery() throws QueryException, IOException;

    boolean createDB();

    QueryResult getMoreResults(boolean streaming) throws QueryException;

    boolean hasUnreadData();
    boolean checkIfMaster() throws QueryException ;
    boolean hasWarnings();
    int getDatatypeMappingFlags();
    StreamingSelectResult getActiveResult();

    void setMaxRows(int max) throws QueryException;
    void setInternalMaxRows(int max);
    int getMaxRows();

    int getMajorServerVersion();

    int getMinorServerVersion();

    boolean versionGreaterOrEqual(int major, int minor, int patch);

    void setLocalInfileInputStream(InputStream inputStream);

    int getMaxAllowedPacket();

    void setMaxAllowedPacket(int maxAllowedPacket);

    void setTimeout(int timeout) throws SocketException;

    int getTimeout() throws SocketException;

    String getPinGlobalTxToPhysicalConnection();
    long getServerThreadId();
    void setTransactionIsolation(int level) throws QueryException;
    int getTransactionIsolationLevel();
}
