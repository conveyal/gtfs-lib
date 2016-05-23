/* This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public License
 as published by the Free Software Foundation, either version 3 of
 the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import org.mapdb.Fun;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Frequency extends Entity implements Comparable<Frequency> {

    public String trip_id;
    public int start_time;
    public int end_time;
    public int headway_secs;
    public int exact_times;

    /** must have a comparator since they go in a navigable set that is serialized */
    @Override
    public int compareTo(Frequency o) {
        return this.start_time - o.start_time;
    }

    public static class Loader extends Entity.Loader<Frequency> {

        public Loader(GTFSFeed feed) {
            super(feed, "frequencies");
        }

        @Override
        public void loadOneRow() throws IOException {
            Frequency f = new Frequency();
            Trip trip = getRefField("trip_id", true, feed.trips);
            f.trip_id = trip.trip_id;
            f.start_time = getTimeField("start_time", true);
            f.end_time = getTimeField("end_time", true);
            f.headway_secs = getIntField("headway_secs", true, 1, 24 * 60 * 60);
            f.exact_times = getIntField("exact_times", false, 0, 1);
            f.feed = feed;
            feed.frequencies.add(Fun.t2(f.trip_id, f));
        }
    }

    public static class Writer extends Entity.Writer<Frequency> {
        public Writer (GTFSFeed feed) {
            super(feed, "frequencies");
        }

        @Override
        public void writeHeaders() throws IOException {
            writer.writeRecord(new String[] {"trip_id", "start_time", "end_time", "headway_secs", "exact_times"});
        }

        @Override
        public void writeOneRow(Frequency f) throws IOException {
            writeStringField(f.trip_id);
            writeTimeField(f.start_time);
            writeTimeField(f.end_time);
            writeIntField(f.headway_secs);
            writeIntField(f.exact_times);
            endRecord();
        }

        @Override
        public Iterator<Frequency> iterator() {
            return feed.frequencies.stream()
                    .map(t2 -> t2.b)
                    .iterator();
        }


    }

}
