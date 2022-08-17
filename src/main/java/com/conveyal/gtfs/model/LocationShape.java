package com.conveyal.gtfs.model;

import com.conveyal.gtfs.GTFSFeed;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocationShape extends Entity {

    private static final Logger LOG = LoggerFactory.getLogger(LocationShape.class);

    private static final long serialVersionUID = -972419107947161195L;
    public static final String polygonCornerCountErrorMessage =
        "Polygon does not have the required number of corners. Four corners are required as a minimum.";

    public String location_id;
    public String geometry_id;
    public double geometry_pt_lat;
    public double geometry_pt_lon;

    public LocationShape() {
    }

    @Override
    public String getId() {
        return location_id;
    }

    public String getGeometry_id() {
        return geometry_id;
    }

    /**
     * Sets the parameters for a prepared statement following the parameter order defined in
     * {@link com.conveyal.gtfs.loader.Table#PATTERN_STOP}. JDBC prepared statement parameters use a one-based index.
     */
    @Override
    public void setStatementParameters(PreparedStatement statement, boolean setDefaultId) throws SQLException {
        int oneBasedIndex = 1;
        if (!setDefaultId) statement.setInt(oneBasedIndex++, id);
        statement.setString(oneBasedIndex++, location_id);
        statement.setString(oneBasedIndex++, geometry_id);
        statement.setDouble(oneBasedIndex++, geometry_pt_lat);
        statement.setDouble(oneBasedIndex++, geometry_pt_lon);
    }

    /**
     * This load method is required by {@link GTFSFeed#loadFromFile(ZipFile, String)}
     */
    public static class Loader extends Entity.Loader<LocationShape> {

        public Loader(GTFSFeed feed) {
            super(feed, "location_shapes");
        }

        @Override
        protected boolean isRequired() {
            return false;
        }

        @Override
        public void loadOneRow() throws IOException {
            LocationShape locationShape = new LocationShape();
            locationShape.id = row + 1; // offset line number by 1 to account for 0-based row index
            locationShape.location_id = getStringField("location_id", true);
            locationShape.geometry_id = getStringField("geometry_id", true);
            locationShape.geometry_pt_lat = getDoubleField("geometry_pt_lat", true, -90D, 90D); // reuse lat/lon min and max from Stop class
            locationShape.geometry_pt_lon = getDoubleField("geometry_pt_lon", true, -180D, 180D);

            // Location id can not be used here because it is not unique.
            feed.locationShapes.put(Integer.toString(row), locationShape);
        }
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry, List)}
     * as part of the unpacking of GeoJson data to CSV.
     */
    public static String header() {
        return "location_id,geometry_id,geometry_pt_lat,geometry_pt_lon\n";
    }

    /**
     * Required by {@link com.conveyal.gtfs.util.GeoJsonUtil#getCsvReaderFromGeoJson(String, ZipFile, ZipEntry, List)}
     * as part of the unpacking of GeoJson data to CSV.
     */
    public String toCsvRow() {
        return String.join(
            ",",
            location_id,
            geometry_id,
            Double.toString(geometry_pt_lat),
            Double.toString(geometry_pt_lon)
        ) + System.lineSeparator();
    }

    /**
     * Validate location geometry types. Specification: https://www.rfc-editor.org/rfc/rfc7946#section-3.1.6.
     */
    public static JsonNode validate(JsonNode jsonNode) throws IOException {
        JsonNode geometryType = jsonNode.get("geometry_type");
        String type = geometryType.asText();
        switch (type) {
            case "polygon":
                return validatePolygon(jsonNode);
            // TODO: Add other geometry types when they are supported.
            default:
                throw new IOException(String.format("Geometry type: %s, is not supported.", type));
        }
    }

    /**
     * A valid polygon must have at least four corners and the first and last corner must be the same. If only three
     * corners are provided and the first and last corners match an error is thrown. If more than three corners are
     * provided and the first and last corners do not match, an additional corner matching the first is added to the end
     * to close the polygon. If four or more corners are provided and the first and last corners match, the location
     * shape is returned unaltered.
     */
    private static JsonNode validatePolygon(JsonNode jsonNode) throws IOException {
        ArrayNode locationShapes = (ArrayNode) jsonNode.get("location_shapes");
        Iterator<JsonNode> corners = locationShapes.elements();
        ObjectNode firstCorner = null;
        ObjectNode lastCorner = null;
        int count = 0;
        while (corners.hasNext()) {
            JsonNode corner = corners.next();
            if (++count == 1) {
                firstCorner = getCorner(corner);
            } else {
                lastCorner = getCorner(corner);
            }
        }
        if (count <= 2) {
            throw new IOException(polygonCornerCountErrorMessage);
        }

        boolean cornersMatch = areMatchingCorners(firstCorner, lastCorner);
        if (cornersMatch && count == 3) {
            // Polygon has been closed, but there are not enough corners provided.
            throw new IOException(polygonCornerCountErrorMessage);
        } else if (!cornersMatch) {
            // Add a new corner to the end of the array matching the first corner. Increment the last corner id by one
            // to create a unique id for this corner.
            int lastCornerId = lastCorner.get("id").asInt();
            firstCorner.put("id", ++lastCornerId);
            locationShapes.add(firstCorner);
            ((ObjectNode) jsonNode).set("location_shapes", locationShapes);
            LOG.warn("An additional corner was added to close a polygon: ({}).", firstCorner);
        }
        return jsonNode;
    }

    /**
     * Compare two shape corners. Corners are considered the same if the lat/lon values match.
     */
    private static boolean areMatchingCorners(ObjectNode firstCorner, ObjectNode lastCorner) {
        return
            firstCorner != null && lastCorner != null &&
            firstCorner.get("geometry_pt_lat").asText().equals(lastCorner.get("geometry_pt_lat").asText()) &&
            firstCorner.get("geometry_pt_lon").asText().equals(lastCorner.get("geometry_pt_lon").asText());
    }

    /**
     * Extract and hold a corner in an {@link ObjectNode}. An {@link ObjectNode} is preferred over an {@link JsonNode}
     * because it is mutable.
     */
    private static ObjectNode getCorner(JsonNode shape) {
        ObjectNode corner = JsonNodeFactory.instance.objectNode();
        corner.put("id", shape.get("id").asText());
        corner.put("location_id", shape.get("location_id").asText());
        corner.put("geometry_id", shape.get("geometry_id").asText());
        corner.put("geometry_pt_lat", shape.get("geometry_pt_lat").asText());
        corner.put("geometry_pt_lon", shape.get("geometry_pt_lon").asText());
        return corner;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationShape that = (LocationShape) o;
        return Double.compare(that.geometry_pt_lat, geometry_pt_lat) == 0 &&
            Double.compare(that.geometry_pt_lon, geometry_pt_lon) == 0 &&
            Objects.equals(location_id, that.location_id) &&
            Objects.equals(geometry_id, that.geometry_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location_id, geometry_id, geometry_pt_lat, geometry_pt_lon);
    }
}


