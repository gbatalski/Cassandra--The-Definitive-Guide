package com.cassandraguide.rw;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

/**
 * OUPUT:
 *
 * Defining new keyspace.
 * Defining new cf.
 * All done.
 */

/**
 * Shows how to define a keyspace and CF programmatically.
 */
public class DefineKeyspaceExample {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws UnsupportedEncodingException,
            InvalidRequestException, UnavailableException, TimedOutException,
            TException, NotFoundException, InterruptedException, SchemaDisagreementException {

        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        System.out.println("Defining new keyspace.");

        KsDef ksdef = new KsDef();
        ksdef.name = "ProgKS";
        ksdef.replication_factor = 1;
        ksdef.strategy_class = "org.apache.cassandra.locator.SimpleStrategy";

        List<CfDef> cfdefs = new ArrayList<CfDef>();
        CfDef cfdef1 = new CfDef();
        cfdef1.name = "ProgCF1";
        cfdef1.keyspace = ksdef.name;
        cfdefs.add(cfdef1);

        ksdef.cf_defs = cfdefs;

        try {
            client.system_add_keyspace(ksdef);
        } catch (InvalidRequestException e) {
            System.out.println("Error: " + e.getWhy());
            return;
        }

        System.out.println("Defining new cf.");
        CfDef cfdef2 = new CfDef();
        cfdef2.keyspace = ksdef.name;
        cfdef2.column_type = "Standard";
        cfdef2.name = "ProgCF";

        client.system_add_column_family(cfdef2);

        conn.close();

        System.out.println("All done.");
    }
}
