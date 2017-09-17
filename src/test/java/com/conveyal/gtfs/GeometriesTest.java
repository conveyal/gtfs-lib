package com.conveyal.gtfs;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import static com.conveyal.gtfs.Geometries.geometryFactory;
import static com.conveyal.gtfs.Geometries.getNetherlandsWithoutTexel;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class GeometriesTest {

    @Test
    public void canGetNetherlandsWithoutTexel() {
        Geometry geom = getNetherlandsWithoutTexel();
        assertThat(
            geom.contains(geometryFactory.createPoint(new Coordinate(4.907812, 52.317809))),
            equalTo(true)
        );
        assertThat(
            geom.contains(geometryFactory.createPoint(new Coordinate(4.816163, 53.099519))),
            equalTo(false)
        );
    }
}
