package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.util.Util;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents a collection of GTFS shape points. Never saved in MapDB but constructed on the fly.
 */
public class Shape {
    private static final Logger LOG = LoggerFactory.getLogger(Shape.class);

    /** The shape itself */
    public LineString geometry;

    /** shape_dist_traveled for each point in the geometry. TODO how to handle shape dist traveled not specified, or not specified on all stops? */
    public double[] shape_dist_traveled;

    public Shape (GTFSFeed feed, String shape_id) {
        Map<Fun.Tuple2<String, Integer>, ShapePoint> points =
                feed.shape_points.subMap(new Fun.Tuple2(shape_id, null), new Fun.Tuple2(shape_id, Fun.HI));

        Coordinate[] coords = points.values().stream()
                .map(point -> new Coordinate(point.shape_pt_lon, point.shape_pt_lat))
                .toArray(i -> new Coordinate[i]);
        geometry = Util.geometryFactory.createLineString(coords);
        shape_dist_traveled = points.values().stream().mapToDouble(point -> point.shape_dist_traveled).toArray();
    }

    /**
     * Provide a list of shape ids that are used more than once in either patterns or trips.
     */
    public static Set<String> getShapeIdsUsedMoreThanOnce(
        Connection connection,
        String patternOrTripTable
    ) throws SQLException {
        String sql =
            String.format(
                "select shape_id from %s group by shape_id having count (shape_id) > 1",
                patternOrTripTable
            );
        return getListOfShapeIds(connection,sql);
    }

    /**
     * Get all shape id associated with a pattern or route.
     */
    public static Set<String> getShapeIdsForRouteOrPattern(
        Connection connection,
        String tablePrefix,
        String routeOrPatternIdColumn,
        String patternsTable,
        String shapesTable,
        String routeOrPatternId) throws SQLException {

        String sql = (routeOrPatternIdColumn.equals("pattern_id"))
            ? String.format(
            "select shape_id from %s s join %s p using (shape_id) where s.shape_id = p.shape_id and p.pattern_id = '%s'",
            shapesTable,
            patternsTable,
            routeOrPatternId
        )
            : String.format(
            "select s.shape_id from %s s inner join %s p on s.shape_id = p.shape_id inner join %s r on p.route_id = r.route_id and r.route_id = '%s'",
            shapesTable,
            patternsTable,
            String.format("%s.routes", tablePrefix),
            routeOrPatternId
        );

        return getListOfShapeIds(connection, sql);
    }

    /**
     * Extract and return a list of shape ids from the provided sql.
     */
    private static Set<String> getListOfShapeIds(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        LOG.info("{}", statement);
        ResultSet resultSet = statement.executeQuery();
        Set<String> shapeIds = new HashSet<>();
        while (resultSet.next()) {
            String shapeId = resultSet.getString(1);
            shapeIds.add(shapeId);
        }
        return shapeIds;
    }

}
