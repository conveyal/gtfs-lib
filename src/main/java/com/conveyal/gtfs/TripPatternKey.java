package com.conveyal.gtfs;

import com.conveyal.gtfs.model.StopTime;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.List;

/**
 * Used as a map key when grouping trips by stop pattern. Note that this includes the routeId, so the same sequence of
 * stops on two different routes makes two different patterns.
 * These objects are not intended for use outside the grouping process.
 */
public class TripPatternKey {

    public String routeId;
    public List<String> stops = new ArrayList<>();
    public TIntList pickupTypes = new TIntArrayList();
    public TIntList dropoffTypes = new TIntArrayList();
    // Note, the lists below are not used in the equality check.
    public TIntList arrivalTimes = new TIntArrayList();
    public TIntList departureTimes = new TIntArrayList();
    public TIntList timepoints = new TIntArrayList();
    public TIntList continuous_pickup = new TIntArrayList();
    public TIntList continuous_drop_off = new TIntArrayList();
    public TDoubleList shapeDistances = new TDoubleArrayList();

    public TripPatternKey (String routeId) {
        this.routeId = routeId;
    }

    public void addStopTime (StopTime st) {
        stops.add(st.stop_id);
        pickupTypes.add(st.pickup_type);
        dropoffTypes.add(st.drop_off_type);
        // Note, the items listed below are not used in the equality check.
        arrivalTimes.add(st.arrival_time);
        departureTimes.add(st.departure_time);
        timepoints.add(st.timepoint);
        shapeDistances.add(st.shape_dist_traveled);
        continuous_pickup.add(st.continuous_pickup);
        continuous_drop_off.add(st.continuous_drop_off);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TripPatternKey that = (TripPatternKey) o;

        if (dropoffTypes != null ? !dropoffTypes.equals(that.dropoffTypes) : that.dropoffTypes != null) return false;
        if (pickupTypes != null ? !pickupTypes.equals(that.pickupTypes) : that.pickupTypes != null) return false;
        if (routeId != null ? !routeId.equals(that.routeId) : that.routeId != null) return false;
        if (stops != null ? !stops.equals(that.stops) : that.stops != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = routeId != null ? routeId.hashCode() : 0;
        result = 31 * result + (stops != null ? stops.hashCode() : 0);
        result = 31 * result + (pickupTypes != null ? pickupTypes.hashCode() : 0);
        result = 31 * result + (dropoffTypes != null ? dropoffTypes.hashCode() : 0);
        return result;
    }

}
