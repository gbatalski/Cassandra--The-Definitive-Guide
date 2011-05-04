package com.cassandraguide.hotel;

import static com.cassandraguide.hotel.Constants.CL;
import static com.cassandraguide.hotel.Constants.*;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;

/*
 * NOTE: Create keyspace "Hotelier" before running HotelApp
 *
 * [default@unknown] create keyspace Hotelier;
 *
 */

/**
 * OUPUT:
 *
 * DEBUG 20:25:23,282 Inserted AZC_043
 * DEBUG 20:25:23,286 Inserted AZS_011
 * DEBUG 20:25:23,288 Inserted CAS_021
 * DEBUG 20:25:23,289 Inserted NYN_042
 * DEBUG 20:25:23,291 Done inserting at 2046829587193
 * DEBUG 20:25:23,299 Inserted HotelByCity index for Cambria Suites Hayden
 * DEBUG 20:25:23,301 Inserted HotelByCity index for Clarion Scottsdale Peak
 * DEBUG 20:25:23,302 Inserted HotelByCity index for The W SF
 * DEBUG 20:25:23,307 Inserted HotelByCity index for The Waldorf=Astoria
 * DEBUG 20:25:23,307 Inserting POIs.
 * DEBUG 20:25:23,310 Done inserting Empire State.
 * DEBUG 20:25:23,312 Done inserting Central Park.
 * DEBUG 20:25:23,314 Done inserting Phoenix Zoo.
 * DEBUG 20:25:23,315 Done inserting Spring Training.
 * DEBUG 20:25:23,316 Done inserting POIs.
 * DEBUG 20:25:23,316 ** Database filled. **
 * DEBUG 20:25:23,318 ** Starting hotel reservation app. **
 * DEBUG 20:25:23,318 Seaching for hotels in Scottsdale, AZ
 * DEBUG 20:25:23,335 Using key java.nio.HeapByteBuffer[pos=43 lim=56 cap=316]
 * DEBUG 20:25:23,335 Found hotel result for Cambria Suites Hayden
 * DEBUG 20:25:23,335 Found hotel result for Clarion Scottsdale Peak
 * DEBUG 20:25:23,336 Using key java.nio.HeapByteBuffer[pos=176 lim=187 cap=316]
 * DEBUG 20:25:23,336 Found hotel result for The Waldorf=Astoria
 * DEBUG 20:25:23,336 Using key java.nio.HeapByteBuffer[pos=252 lim=268 cap=316]
 * DEBUG 20:25:23,336 Found hotel result for The W SF
 * DEBUG 20:25:23,336 Found hotels in city. Results: 4
 * DEBUG 20:25:23,337 You picked Cambria Suites Hayden
 * DEBUG 20:25:23,337 Finding Points of Interest near Cambria Suites Hayden
 * DEBUG 20:25:23,340 Found something neat nearby: Phoenix Zoo.
 * Desc: null.
 * Phone: null
 * DEBUG 20:25:23,340 Found something neat nearby: Spring Training.
 * Desc: null.
 * Phone: null
 * DEBUG 20:25:23,340 Hm... Phoenix Zoo. null--Sounds fun!
 * DEBUG 20:25:23,340 Now to book a room...
 * DEBUG 20:25:23,341 All done.
 */

/**
 * Runs the hotel application. After the database is pre-populated, this class
 * mocks a user interaction to perform a hotel search based on city, selects
 * one, then looks at some surrounding points of interest for that hotel.
 *
 * Shows using Materialized View pattern, get, get_range_slices, key slices.
 *
 * These exceptions are thrown out of main to reduce code size:
 * UnsupportedEncodingException, InvalidRequestException, UnavailableException,
 * TimedOutException, TException, NotFoundException, InterruptedException
 *
 * Uses the Constants class for some commonly used strings.
 */
public class HotelApp {
    private static final Logger LOG = Logger.getLogger(HotelApp.class);

    public static void main(String[] args) throws Exception {

        // first put all of the data in the database
        new Prepopulate().prepopulate();
        LOG.debug("** Database filled. **");

        // now run our client
        LOG.debug("** Starting hotel reservation app. **");
        HotelApp app = new HotelApp();

        // find a hotel by city--try Scottsdale or New York...
        List<Hotel> hotels = app.findHotelByCity("Scottsdale", "AZ");
        // List<Hotel> hotels = app.findHotelByCity("New York", "NY");
        LOG.debug("Found hotels in city. Results: " + hotels.size());

        // choose one
        Hotel h = hotels.get(0);

        LOG.debug("You picked " + h.name);

        // find Points of Interest for selected hotel
        LOG.debug("Finding Points of Interest near " + h.name);
        List<POI> points = app.findPOIByHotel(h.name);

        // choose one
        if (!points.isEmpty()) {
            POI poi = points.get(0);
            LOG.debug("Hm... " + poi.name + ". " + poi.desc + "--Sounds fun!");
        }

        LOG.debug("Now to book a room...");

        // show availability for a date
        // left as an exercise...

        // create reservation
        // left as an exercise...

        LOG.debug("All done.");
    }

    // use column slice to get from Super Column
    public List<POI> findPOIByHotel(String hotel) throws Exception {

        // /query
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(hotel.getBytes());
        sliceRange.setFinish(hotel.getBytes());
        predicate.setSlice_range(sliceRange);
        
    
       
        // read all columns in the row
        String scFamily = "PointOfInterest";
        ColumnParent parent = new ColumnParent(scFamily);

        KeyRange keyRange = new KeyRange();
        keyRange.start_key = bytes("");
        keyRange.end_key = bytes("");

        List<POI> pois = new ArrayList<POI>();

        // instead of a simple list, we get a map whose keys are row keys
        // and the values the list of columns returned for each
        // only row key + first column are indexed
        Connector cl = new Connector();
        Cassandra.Client client = cl.connect();
        client.set_keyspace(KEYSPACE);
        
        List<KeySlice> slices = client.get_range_slices(parent, predicate,
                keyRange, CL);

        for (KeySlice slice : slices) {
            List<ColumnOrSuperColumn> cols = slice.columns;

            POI poi = new POI();
            poi.name = new String(ByteBufferUtil.string(slice.key));

            for (ColumnOrSuperColumn cosc : cols) {
                SuperColumn sc = cosc.super_column;

                List<Column> colsInSc = sc.columns;

                for (Column c : colsInSc) {
                    String colName = new String(c.getName(), UTF8);
                    if (colName.equals("desc")) {
                        poi.desc = new String(c.getValue(), UTF8);
                    }
                    if (colName.equals("phone")) {
                        poi.phone = new String(c.getValue(), UTF8);
                    }
                }

                LOG.debug("Found something neat nearby: " + poi.name
                        + ". \nDesc: " + poi.desc + ". \nPhone: " + poi.phone);
                pois.add(poi);
            }
        }

        cl.close();
        return pois;
    }

    // uses key range
    public List<Hotel> findHotelByCity(String city, String state)
            throws Exception {

        LOG.debug("Seaching for hotels in " + city + ", " + state);

        String key = city + ":" + state.toUpperCase();

        // /query
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        // read all columns in the row
        String columnFamily = "HotelByCity";
        ColumnParent parent = new ColumnParent(columnFamily);

        KeyRange keyRange = new KeyRange();
        keyRange.setStart_key(key.getBytes());
        keyRange.setEnd_key("".getBytes()); // just outside lexical range
        keyRange.count = 5;

        Connector cl = new Connector();
        Cassandra.Client client = cl.connect();
        client.set_keyspace(KEYSPACE);
        List<KeySlice> keySlices = client.get_range_slices(parent, predicate,
                keyRange, CL);

        List<Hotel> results = new ArrayList<Hotel>();

        for (KeySlice ks : keySlices) {
            List<ColumnOrSuperColumn> coscs = ks.columns;
            LOG.debug(new String("Using key " + ks.key));

            for (ColumnOrSuperColumn cs : coscs) {

                Hotel hotel = new Hotel();
                hotel.name = ByteBufferUtil.string(cs.column.name);
                hotel.city = city;
                hotel.state = state;

                results.add(hotel);
                LOG.debug("Found hotel result for " + hotel.name);
            }
        }
        // /end query
        cl.close();

        return results;
    }
}
