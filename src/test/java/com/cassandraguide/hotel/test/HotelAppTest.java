package com.cassandraguide.hotel.test;

import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;

import org.junit.Before;

import org.junit.Test;

import com.cassandraguide.hotel.*;

import static com.cassandraguide.hotel.CassandraUtils.*;
import static com.cassandraguide.hotel.Constants.KEYSPACE;

public class HotelAppTest {

    private Connector connector = new Connector();
    private Client client;
    private HotelApp app = new HotelApp();

    /**
     * Create necessary schema
     * 
     * @throws InvalidRequestException
     * @throws TException
     * @throws TTransportException
     */
    @Before
    public void setUp() throws TTransportException, TException,
            InvalidRequestException {

        client = connector.connect();

        createKeyspace(client, KEYSPACE);

        client.set_keyspace(KEYSPACE);

        createColumnFamily(client, KEYSPACE, "Hotel", "HotelByCity");
        createSuperColumnFamily(client, KEYSPACE, "PointOfInterest");

    }

    @Test
    public void populateData() throws Exception {
        fillData();
    }

    @Test
    public void findHotelByCity() throws Exception {
        fillData();
        List<Hotel> hotels = app.findHotelByCity("Scottsdale", "AZ");
        Assert.assertNotNull(hotels.isEmpty());
        Assert.assertFalse(hotels.isEmpty());
    }

    @Test
    public void findPOIByHotel() throws Exception {
        fillData();
        Hotel h = app.findHotelByCity("Scottsdale", "AZ").get(0);
        List<POI> pois = app.findPOIByHotel(h.name);
        Assert.assertNotNull(pois);
        Assert.assertFalse(pois.isEmpty());
    }

    /**
     * clean up cassandra from schema
     */
    @After
    public void teerDown() {
        dropKeySpace(client, KEYSPACE);
        connector.close();
    }

}
