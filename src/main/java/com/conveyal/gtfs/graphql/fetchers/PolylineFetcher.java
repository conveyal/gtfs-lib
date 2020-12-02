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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * GraphQL fetcher to get encoded polylines for all shapes in a GTFS feed.
 *
 * Note: this fetcher was developed to prevent out of memory errors associated with requesting all trip patterns with
 * their associated shapes. Previously, attempting to fetch all shapes for a large feed would require a separate SQL
 * query for each shape (to join on shape_id) and result in many duplicated shapes in the response.
 *
 * Running a shapes polyline query on a 2017 Macbook Pro (2.5 GHz Dual-Core Intel Core i7) results in the following:
 * - NL feed (11291 shapes): ~12 seconds (but nobody should be editing such a large feed in the editor)
 * - TriMet (1302 shapes): ~8 seconds
 * - MBTA (1272 shapes): ~4 seconds
 */
public class PolylineFetcher implements DataFetcher {
    public static final Logger LOG = LoggerFactory.getLogger(PolylineFetcher.class);

    @Override
    public Object get(DataFetchingEnvironment environment) {
        GeometryFactory gf = new GeometryFactory();
        List<Shape> shapes = new ArrayList<>();
        Map<String, Object> parentFeedMap = environment.getSource();
        String namespace = (String) parentFeedMap.get("namespace");
        Connection connection = null;
        try {
            connection = GTFSGraphQL.getConnection();
            Statement statement = connection.createStatement();
            String sql = String.format(
                "select shape_id, shape_pt_lon, shape_pt_lat from %s.shapes order by shape_id, shape_pt_sequence",
                namespace
            );
            LOG.info("SQL: {}", sql);
            if (statement.execute(sql)) {
                ResultSet result = statement.getResultSet();
                String currentShapeId = null;
                String nextShapeId;
                List<Point> shapePoints = new ArrayList<>();
                while (result.next()) {
                    // Get values from SQL row.
                    nextShapeId = result.getString(1);
                    double lon = result.getDouble(2);
                    double lat = result.getDouble(3);
                    if (currentShapeId != null && !nextShapeId.equals(currentShapeId)) {
                        // Finish current shape if new shape_id is encountered.
                        shapes.add(new Shape(currentShapeId, shapePoints));
                        // Start building new shape.
                        shapePoints = new ArrayList<>();
                    }
                    // Update current shape_id and add shape point to list.
                    currentShapeId = nextShapeId;
                    shapePoints.add(gf.createPoint(new Coordinate(lon, lat)));
                }
                // Add the final shape when result iteration is finished.
                shapes.add(new Shape(currentShapeId, shapePoints));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
        return shapes;
    }

    /**
     * Simple class to return shapes for GraphQL response format.
     */
    public static class Shape {
        public String shape_id;
        public String polyline;

        public Shape(String shape_id, List<Point> shapePoints) {
            this.shape_id = shape_id;
            // Encode the shapepoints as a polyline
            this.polyline = PolylineUtils.encode(shapePoints, 6);
        }
    }
}
