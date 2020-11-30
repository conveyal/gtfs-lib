package com.conveyal.gtfs.util;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.conveyal.gtfs.util.PolylineUtils.decode;
import static com.conveyal.gtfs.util.PolylineUtils.encode;
import static com.conveyal.gtfs.util.PolylineUtils.simplify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PolylineUtilsTest {

    private static final int PRECISION_6 = 6;
    private static final int PRECISION_5 = 5;

    // Delta for Coordinates comparison
    private static final double DELTA = 0.000001;

    private static final String SIMPLIFICATION_INPUT = "simplification-input";
    private static final String SIMPLIFICATION_EXPECTED_OUTPUT = "simplification-expected-output";

    private static final String TEST_LINE
        = "_cqeFf~cjVf@p@fA}AtAoB`ArAx@hA`GbIvDiFv@gAh@t@X\\|@z@`@Z\\Xf@Vf@VpA\\tATJ@NBBkC";

    private static final  String TEST_LINE6 =
        "qn_iHgp}LzCy@xCsAsC}PoEeD_@{A@uD_@Sg@Je@a@I_@FcAoFyGcCqFgQ{L{CmD";

    private static final GeometryFactory gf = new GeometryFactory();

    @Test
    public void testDecodePath() {
        List<Point> latLngs = decode(TEST_LINE, PRECISION_5);

        int expectedLength = 21;
        assertEquals("Wrong length.", expectedLength, latLngs.size());

        Point lastPoint = latLngs.get(expectedLength - 1);
//        expectNearNumber(37.76953, lastPoint.getY(), 1e-6);
//        expectNearNumber(-122.41488, lastPoint.getX(), 1e-6);
    }

    @Test
    public void testEncodePath5() {
        List<Point> path = decode(TEST_LINE, PRECISION_5);
        String encoded = encode(path, PRECISION_5);
        assertEquals(TEST_LINE, encoded);
    }

    @Test
    public void testDecodeEncodePath6() {
        List<Point> path = decode(TEST_LINE6, PRECISION_6);
        String encoded = encode(path, PRECISION_6);
        assertEquals(TEST_LINE6, encoded);
    }

    @Test
    public void testEncodeDecodePath6() {
        List<Point> originalPath = Arrays.asList(
            gf.createPoint(new Coordinate(2.2862036, 48.8267868)),
            gf.createPoint(new Coordinate(2.4, 48.9))
        );

        String encoded = encode(originalPath, PRECISION_6);
        List<Point> path =  decode(encoded, PRECISION_6);
        assertEquals(originalPath.size(), path.size());

        for (int i = 0; i < originalPath.size(); i++) {
            assertEquals(originalPath.get(i).getY(), path.get(i).getY(), DELTA);
            assertEquals(originalPath.get(i).getX(), path.get(i).getX(), DELTA);
        }
    }


    @Test
    public void decode_neverReturnsNullButRatherAnEmptyList() throws Exception {
        List<Point> path = decode("", PRECISION_5);
        assertNotNull(path);
        assertEquals(0, path.size());
    }

    @Test
    public void encode_neverReturnsNull() throws Exception {
        String encodedString = encode(new ArrayList<Point>(), PRECISION_6);
        assertNotNull(encodedString);
    }

    @Test
    public void simplify_neverReturnsNullButRatherAnEmptyList() throws Exception {
        List<Point> simplifiedPath = simplify(new ArrayList<Point>(), PRECISION_6);
        assertNotNull(simplifiedPath);
    }

    @Test
    public void simplify_returnSameListWhenListSizeIsLessThanOrEqualToTwo(){
        final List<Point> path = new ArrayList<>();
        path.add(gf.createPoint(new Coordinate(0, 0)));
        path.add(gf.createPoint(new Coordinate(10, 0)));
        List<Point> simplifiedPath = simplify(path, PRECISION_6, true);
        assertTrue("Returned list is different from input list", path == simplifiedPath);
    }
}