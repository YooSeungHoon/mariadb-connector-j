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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.common.ParameterConstant;
import org.mariadb.jdbc.internal.common.UrlHAMode;

import java.util.List;
import java.util.Properties;

/**
 * parse and verification of URL.
 *
 * the URL format has 2 Connection syntax :
 *
 * basic syntax :
 * jdbc:(mysql|mariadb):[replication:|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>]/<database>[?<key1>=<value1>&<key2>=<value2>...]
 *
 * hostDescription:
 *      simple :
 *          <host>:<portnumber>
 *          (for example localhost:3306)
 *      complex :
 *           address=[(type=(master|slave))][(port=<portnumber>)](host=<host>)
 *
 *      type is by default master
 *      port is by default 3306
 *
 * Some examples :
 *      jdbc:mysql://localhost:3306/database?user=greg&password=pass
 *      jdbc:mysql://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass
 *
 */
public class JDBCUrl {


    private String username;
    private String password;
    private String database;
    private Properties properties;
    private List<HostAddress> addresses;
    private UrlHAMode haMode;

    protected JDBCUrl(String database, List<HostAddress> addresses, Properties properties, UrlHAMode haMode) {
        if (properties != null) {
            if (properties.getProperty("user") != null) username=properties.getProperty("user");
            if (properties.getProperty("password") != null) password=properties.getProperty("password");
        }
        this.database = database;
        this.addresses = addresses;
        this.properties = properties;
        this.haMode = haMode;

        if (haMode == UrlHAMode.AURORA) {
            for (HostAddress hostAddress : addresses) hostAddress.type = null;
        } else {
            for (HostAddress hostAddress : addresses) {
                if (hostAddress.type == null)hostAddress.type = ParameterConstant.TYPE_MASTER;
            }
        }
    }

    /*
        Parse ConnectorJ compatible urls
        jdbc:mysql://host:port/database
        Example: jdbc:mysql://localhost:3306/test?user=root&password=passwd
         */
    private static JDBCUrl parseConnectorJUrl(String url, Properties properties) {
        if (!url.startsWith("jdbc:mysql:")) return null;

        String[] baseTokens = url.substring(0,url.indexOf("//") - 1).split(":");

        //parse HA mode
        UrlHAMode haMode = UrlHAMode.NONE;
        if (baseTokens.length > 2) {
            try {
                haMode = UrlHAMode.valueOf(baseTokens[2].toUpperCase());
            }catch (IllegalArgumentException i) {
                throw new IllegalArgumentException("url parameter error '" + baseTokens[2] +"' is a unknown parameter in the url "+url);
            }
        }


        url = url.substring(url.indexOf("//") + 2);
        String[] tokens = url.split("/");
        String hostAddressesString= tokens[0];
        String additionalParameters = (tokens.length > 1) ? tokens[1] : null;

        if (additionalParameters == null) {
            return new JDBCUrl(null, HostAddress.parse(hostAddressesString), properties, haMode);
        }
        String database="";
        int ind = additionalParameters.indexOf('?');
        if (ind > -1) {
            database = additionalParameters.substring(0, ind);
            String urlParameters = additionalParameters.substring(ind + 1);
            setUrlParameters(urlParameters, properties);
        }

        return new JDBCUrl(database, HostAddress.parse(hostAddressesString), properties, haMode);
    }

    static boolean acceptsURL(String url) {
        return (url != null) &&
                (url.startsWith("jdbc:mariadb://") || url.startsWith("jdbc:mysql://"));

    }

    public static JDBCUrl parse(final String url) {
        return parse(url, new Properties());
    }

    public static JDBCUrl parse(final String url, Properties prop) {
        if (url != null) {
            if (prop == null) prop = new Properties();
            if (url.startsWith("jdbc:mysql:")) {
                return parseConnectorJUrl(url, prop);
            }
            String[] arr = new String[]{"jdbc:mysql:thin:", "jdbc:mariadb:"};
            for (String prefix : arr) {
                if (url.startsWith(prefix)) {
                    return parseConnectorJUrl("jdbc:mysql:" + url.substring(prefix.length()), prop);
                }
            }
        }
        throw new IllegalArgumentException("Invalid connection URL url " + url);
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public List<HostAddress> getHostAddresses() {
        return this.addresses;
    }

    public Properties getProperties() {
        return properties;
    }

    protected void setUsername(String username) {
        this.username = username;
    }

    protected void setPassword(String password) {
        this.password = password;
    }

    protected void setDatabase(String database) {
        this.database = database;
    }

    protected void setProperties(String urlParameters) {
        setUrlParameters(urlParameters, this.properties);
        if (properties.getProperty("user") != null)this.username = properties.getProperty("user");
        if (properties.getProperty("password") != null)this.password = properties.getProperty("password");
    }

    public String toString() {
        String s = "jdbc:mysql://";
        if (!haMode.equals(UrlHAMode.NONE)) s = "jdbc:mysql:"+haMode.toString().toLowerCase()+"://";
        if (addresses != null)
            s += HostAddress.toString(addresses);
        if (database != null)
            s += "/" + database;
        return s;
    }

    /**
     * Adds the parsed parameter to the properties object.
     *
     * @param parameter a key=value pair
     * @param info the properties object
     */
    private static void setUrlParameter(String parameter, Properties info) {
        int pos = parameter.indexOf('=');
        if (pos == -1)  {
            throw new IllegalArgumentException("Invalid connection URL, expected key=value pairs, found " + parameter);
        }
        info.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
    }

    /**
     * Parses the parameters string and sets the corresponding properties in the properties object.
     *
     * @param urlParameters the parameters string
     * @param info the properties object
     */
    private static void setUrlParameters(String urlParameters, Properties info) {
        String [] parameters = urlParameters.split("&");
        for(String parameter : parameters) {
            setUrlParameter(parameter, info);
        }
    }

    public UrlHAMode getHaMode() {
        return haMode;
    }

}
