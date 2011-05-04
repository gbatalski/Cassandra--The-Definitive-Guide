package com.cassandraguide.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ConfigAPI {

    private static final String HOST = "localhost";
    private static final int PORT = 9160;

    /**
     * Creates a new keyspace and CF.
     */
    @SuppressWarnings("deprecation")
    public static void main(String... args) throws Exception {

        String keyspaceName = "DynamicKeyspace";
        System.out.println("Creating new keyspace: " + keyspaceName);

        // Create Keyspace
        KsDef k = new KsDef();
        k.setName(keyspaceName);
        k.setReplication_factor(1);
        k.setStrategy_class("org.apache.cassandra.locator.SimpleStrategy");

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        k.setCf_defs(cfDefs);

        // Connect to Server
        TTransport tr = new TSocket(HOST, PORT);
        TFramedTransport tf = new TFramedTransport(tr);
        TProtocol proto = new TBinaryProtocol(tf);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        // Add the new keyspace
        try {
            client.system_add_keyspace(k);
            System.out.println("Added keyspace: " + keyspaceName);
        } catch (InvalidRequestException e) {
            System.out.println("Error: " + e.getWhy());
        }
    }
}
