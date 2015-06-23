package org.mariadb.jdbc.multihost;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by diego_000 on 08/06/2015.
 */
public class TcpProxySocket implements Runnable {
    protected static Logger log = Logger.getLogger("org.maria.jdbc");

    String host;
    int remoteport;
    int localport;
    boolean stop = false;
    Socket client = null, server = null;
    ServerSocket ss;

    public TcpProxySocket(String host, int remoteport, int localport) {
        this.host = host;
        this.remoteport = remoteport;
        this.localport = localport;
    }

    public void kill() {
        stop = true;
        try {
            if (server != null) server.close();
        } catch (IOException e) { }
        try {
            if (client != null) client.close();
        } catch (IOException e) { }
        try {
            ss.close();
        } catch (IOException e) { }
    }

    @Override
    public void run() {

        stop = false;
        try {
            ss = new ServerSocket(localport);
            final byte[] request = new byte[1024];
            byte[] reply = new byte[4096];
            while (!stop) {
                try {
                    client = ss.accept();
                    final InputStream from_client = client.getInputStream();
                    final OutputStream to_client = client.getOutputStream();
                    try {
                        server = new Socket(host, remoteport);
                    } catch (IOException e) {
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(to_client));
                        out.println("Proxy server cannot connect to " + host + ":" +
                                remoteport + ":\n" + e);
                        out.flush();
                        client.close();
                        continue;
                    }
                    final InputStream from_server = server.getInputStream();
                    final OutputStream to_server = server.getOutputStream();
                    new Thread() {
                        public void run() {
                            int bytes_read;
                            try {
                                while ((bytes_read = from_client.read(request)) != -1) {
                                    to_server.write(request, 0, bytes_read);
                                    log.finest(bytes_read + "to_server--->" + new String(request, "UTF-8") + "<---");
                                    to_server.flush();
                                }
                            } catch (IOException e) {
                            }
                            try {
                                to_server.close();
                            } catch (IOException e) {
                            }
                        }
                    }.start();
                    int bytes_read;
                    try {
                        while ((bytes_read = from_server.read(reply)) != -1) {
                            try {
                                Thread.sleep(1);
                                log.finest(bytes_read + "to_client--->" + new String(request, "UTF-8") + "<---");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            to_client.write(reply, 0, bytes_read);
                            to_client.flush();
                        }
                    } catch (IOException e) {
                    }
                    to_client.close();
                } catch (IOException e) {
                    //System.err.println("ERROR socket : "+e);
                }
                finally {
                    try {
                        if (server != null) server.close();
                        if (client != null) client.close();
                    } catch (IOException e) {
                    }
                }
            }
        } catch ( IOException e) {
            e.printStackTrace();
        }
    }
}
