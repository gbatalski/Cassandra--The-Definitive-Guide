package com.cassandraguide.hotel;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.util.ArrayList;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraUtils {
    static Logger log = LoggerFactory.getLogger(CassandraUtils.class);

    public static Column buildColumn(String name, String value, long timestamp) {
        Column c = new Column();
        if (name != null)
            c.setName(bytes(name));
        if (value != null)
            c.setValue(bytes(value));

        c.setTimestamp(timestamp);
        return c;
    }

    @SuppressWarnings("deprecation")
    public static void createKeyspace(Client client, String ksName) {
        log.info("creating Keyspace " + ksName);
        KsDef ksDef = new KsDef(ksName, SimpleStrategy.class.getName(),
                new ArrayList<CfDef>());

        ksDef.setReplication_factor(1);
        try {
            client.system_add_keyspace(ksDef);
        } catch (Exception e) {
            log.warn("Creation of Keyspace failed", e);
        }

    }

    public static void dropColumnFamily(Client client, String... cfNames) {
        for (String cfName : cfNames) {
            try {
                client.system_drop_column_family(cfName);
            } catch (Exception e) {

                log.warn("", e);

            }
        }
    }

    public static void dropKeySpace(Client client, String ksName) {
        log.info("dropping Keyspace " + ksName);
        try {
            client.system_drop_keyspace(ksName);
        } catch (Exception e) {

            log.warn("", e);

        }

    }

    public static void createColumnFamily(Client client, String ksName,
            String... cfNames) {
        for (String cfName : cfNames) {
            try {
                log.info("creating column family " + cfName);
                client.system_add_column_family(new CfDef(ksName, cfName));

            } catch (Exception e) {
                log.warn("creating column family failed", e);
            }
        }
    }

    public static void createSuperColumnFamily(Client client, String ksName,
            String... cfNames) {
        for (String cfName : cfNames) {
            try {
                log.info("creating super column family " + cfName);
                CfDef cfDef = new CfDef(ksName, cfName);
                cfDef.setColumn_type("Super");
                client.system_add_column_family(cfDef);

            } catch (Exception e) {
                log.warn("creating super column family failed", e);
            }
        }
    }

    public static void fillData() throws Exception {
        log.info("populating some app data");
        new Prepopulate().prepopulate();
    }

}
