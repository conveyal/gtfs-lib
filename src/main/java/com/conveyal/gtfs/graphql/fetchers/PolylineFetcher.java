package com.conveyal.gtfs.graphql.fetchers;

import com.conveyal.gtfs.graphql.GTFSGraphQL;
import com.conveyal.gtfs.util.PolylineUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.apache.commons.dbutils.DbUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * GraphQL fetcher to get encoded polylines for all shapes in a GTFS feed.
 *
 * Note: this fetcher was developed to prevent out of memory errors associated with requesting all trip patterns with
 * their associated shapes. Previously, attempting to fetch all shapes for a large feed would require a separate SQL
 * query for each shape (to join on shape_id) and result in many duplicated shapes in the response.
 *
 * Running a shapes polyline query on a 2017 Macbook Pro (2.5 GHz Dual-Core Intel Core i7) results in the following:
 * - NL feed (11291 shapes, 4.2M rows): ~13 seconds (but hopefully nobody is editing such a large feed in the editor)
 * - TriMet (1909 shapes, 719K rows): ~3 seconds
 * - MBTA (1272 shapes, 268K rows): 1-2 seconds
 *
 * This was originally handled in a single query to the shapes table that sorted on shape_id and shape_pt_sequence, but
 * per Evan Siroky rec, we've split into two queries to reduce the number of sort operations significantly. For example:
 *
 * There are 719k lines in the TriMet shapes.txt file with 1,909 unique shape IDs (so about 377 points per
 * shape on average). With that, the numbers become:
 *
 * 1 * 719k * log2(719k) = 13.9m operations
 * 1909 * 377 * log2(377) = 6.1m operations
 *
 * NOTE: This doesn't provide much of a benefit for the NL feed (in fact, it appears to have a disbenefit/slowdown
 * of about 1-2 seconds on average), but for the other feeds tested the double query approach was about twice as fast.
 */
public class PolylineFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(PolylineFetcher.class);
    private static final GeometryFactory gf = new GeometryFactory();

    @Override
    public Object get(DataFetchingEnvironment environment) {
        Map<String, Object> parentFeedMap = environment.getSource();
        String namespace = (String) parentFeedMap.get("namespace");
        Connection connection = null;
        try {
            List<Shape> shapes = new ArrayList<>();
            connection = GTFSGraphQL.getConnection();
            // First, collect all shape ids.
            Set<String> shapeIds = new HashSet<>();
            String getShapeIdsSql = String.format("select distinct shape_id from %s.shapes", namespace);
            LOG.info(getShapeIdsSql);
            ResultSet shapeIdsResult = connection.createStatement().executeQuery(getShapeIdsSql);
            while (shapeIdsResult.next()) {
                shapeIds.add(shapeIdsResult.getString(1));
            }
            // Next, iterate over shape ids and build shapes progressively.
            PreparedStatement shapePointsStatement = connection.prepareStatement(String.format(
                "select shape_pt_lon, shape_pt_lat from %s.shapes where shape_id = ? order by shape_pt_sequence",
                namespace
            ));
            for (String shapeId : shapeIds) {
                shapePointsStatement.setString(1, shapeId);
                ResultSet result = shapePointsStatement.executeQuery();
                List<Point> shapePoints = new ArrayList<>();
                while (result.next()) {
                    // Get lon/lat values from SQL row.
                    double lon = result.getDouble(1);
                    double lat = result.getDouble(2);
                    shapePoints.add(gf.createPoint(new Coordinate(lon, lat)));
                }
                // Construct/add shape once all points have been gathered.
                shapes.add(new Shape(shapeId, shapePoints));
            }
            // Finally, return the shapes with encoded polylines.
            return shapes;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    /**
     * Simple class to return shapes for GraphQL response format.
     */
    public static class Shape {
        public final String shape_id;
        public final String polyline;

        public Shape(String shape_id, List<Point> shapePoints) {
            this.shape_id = shape_id;
            // Encode the shapepoints as a polyline
            this.polyline = PolylineUtils.encode(shapePoints, 6);
        }
    }
}
