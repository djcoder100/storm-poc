package com.kiku.gridlattice;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Created by rajan on 06/04/2014.
 */
public class CassandraConnection {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(CassandraConnection.class);

    private static CassandraConnection _instance;
    private static Cluster cluster;
    private static Session session;
    private static String connectionDetails;
    private static String keySpace = "EVENT_POC";

    static {

        if (_instance == null) {
            _instance = new CassandraConnection();
        }
    }

    private CassandraConnection() {

        try {

            // TODO Convert this dynamic configuration
            String serverIP1 = "127.0.0.1";
            //String serverIP2 = "gbl04116.systems.uk.xxx";
            //String serverIP3 = "gbl04117.systems.uk.xxx";
            //String serverIP4 = "gbl04118.systems.uk.xxx";

            LOGGER.info(String.format("Connecting to cluster keyspace=%s....", keySpace));

            //String[] hosts = {"gbl04115.systems.uk.xxx", "gbl04116.systems.uk.xxx", "gbl04117.systems.uk.xxx", "gbl04118.systems.uk.xxx"};
            String[] hosts = {"127.0.0.1"};

            cluster = Cluster.builder()
                    .addContactPoints(hosts)
                    .build();
            //cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(30000);

            session = cluster.connect(keySpace);

            //LOGGER.info(String.format("Connected to cluster=%s hosts=%s",
            //          session.getCluster().getMetadata().getClusterName(),
            //          session.getCluster().getMetadata().getAllHosts()));

            StringBuilder s = new StringBuilder();
            s.append(String.format("Data storage cluster=%s", session.getCluster().getMetadata().getClusterName()));
            Set<Host> allHosts = cluster.getMetadata().getAllHosts();
            for (Host h : allHosts) {
                s.append("[");
                s.append(h.getDatacenter());
                s.append("-");
                s.append(h.getRack());
                s.append("-");
                s.append(h.getAddress());
                s.append("]");
            }

            connectionDetails = s.toString();
            LOGGER.info(String.format("Connected to %s", connectionDetails));

        }
        catch (Exception ex) {
            LOGGER.error(String.format("Failed to connect to Cassandra Cluster for keyspace=$s",keySpace, ex));
            throw new RuntimeException(ex);
        }
    }

    public static void close() {
        if (getCluster() != null) {

            LOGGER.info(String.format("Closing %s ...", connectionDetails));
            getSession().close();
            getCluster().close();
            LOGGER.info(String.format("Closed"));
        }
    }


    public static String getKeySpace() {
        return keySpace;
    }

    public static Cluster getCluster() {
        return cluster;
    }

    public static Session getSession() {
        return session;
    }

    public static ResultSet execute(Statement query) {
        if (query != null) {
            return session.execute(query);
        }
        return null;
    }

    public static ResultSet execute(String query) {
        if (query != null) {
            return session.execute(query);
        }
        return null;
    }

}
