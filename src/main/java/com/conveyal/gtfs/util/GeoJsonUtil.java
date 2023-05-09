package com.conveyal.gtfs.util;

import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationShape;
import com.csvreader.CsvReader;
import com.google.common.collect.ImmutableSet;
import mil.nga.sf.Geometry;
import mil.nga.sf.LineString;
import mil.nga.sf.Point;
import mil.nga.sf.Polygon;
import mil.nga.sf.geojson.Feature;
import mil.nga.sf.geojson.FeatureCollection;
import mil.nga.sf.geojson.FeatureConverter;
import org.apache.commons.io.input.BOMInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.util.stream.Collectors.toList;

/**
 * With the aid of this third party library: https://ngageoint.github.io/simple-features-geojson-java/, this util class
 * handles the unpacking and packing of GeoJson data. Unpacking flattens the location data into two classes
 * {@link Location} and {@link LocationShape}. Packing does the opposite by using these two classes to convert
 * the data back into validate GeoJson.
 */
public class GeoJsonUtil {

    private static final Logger LOG = LoggerFactory.getLogger(GeoJsonUtil.class);
    public static final String GEOMETRY_TYPE_POLYLINE = "polyline";
    public static final String GEOMETRY_TYPE_POLYGON = "polygon";
    private static final String UNSUPPORTED_GEOMETRY_TYPE_MESSAGE = "Geometry type %s unknown or not supported.";
    private static final String STOP_NAME = "stop_name";
    private static final String STOP_DESC = "stop_desc";
    private static final String ZONE_ID = "zone_id";
    private static final String STOP_URL = "stop_url";

    private GeoJsonUtil() {
        throw new IllegalStateException("GeoJson utility class.");
    }

    /**
     * Takes the content of a zip file entry and converts it into a {@link FeatureCollection} which is a class
     * representation of features held in the locations file.
     */
    public static FeatureCollection getLocations(ZipFile zipFile, ZipEntry entry) {
        try (InputStream zipInputStream = zipFile.getInputStream(entry)) {
            String content;
            try (InputStream bomInputStream = new BOMInputStream(zipInputStream)) {
                content = new BufferedReader(
                    new InputStreamReader(bomInputStream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n")
                    );
            }
            return FeatureConverter.toFeatureCollection(content);
        } catch (IOException e) {
            LOG.error("Exception while opening zip entry: ", e);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * The front end uses (for now) geometry types defined by react-leaflet-draw rather than those explicitly in the
     * flex spec. Here we'll just convert the flex spec types to those used by the front end.
     */
    private static String getGeometryType(String geometryType, List<String> errors) {
        switch (geometryType) {
            case "LINESTRING":
                return GEOMETRY_TYPE_POLYLINE;
            case "POLYGON":
                return GEOMETRY_TYPE_POLYGON;
            // TODO: Add additional geometry types.
            default:
                // Effectively, MultiPolygon, Polygon and MultiLineString types aren't supported yet.
                logErrorMessage(
                    String.format(UNSUPPORTED_GEOMETRY_TYPE_MESSAGE, geometryType),
                    errors
                );
                return null;
        }
    }

    /**
     *   Check for features without IDs, with empty geometry or duplicate IDs (this happens more than you think)
     */
    private static boolean isValidLocation(String locationId, String geometryType, Set<String> seenLocationIds) {
        return (locationId == null || geometryType == null || !seenLocationIds.add(locationId)) ? false : true;
    }

    /**
     * Extract from a list of features, the items which are common to all features.
     */
    private static List<Location> unpackLocations(
        FeatureCollection featureCollection,
        List<String> errors
    ) {
        ArrayList<Location> locations = new ArrayList<>();
        Set<String> seenLocationIds = new HashSet<>();
        for (Feature feature : featureCollection.getFeatures()) {
            String geometryType = getGeometryType(feature.getGeometryType().getName(), errors);
            Location location = new Location();
            String locationId = feature.getId();

            if (!isValidLocation(locationId, geometryType, seenLocationIds)) continue;

            location.location_id = locationId;
            location.geometry_type = geometryType;
            extractPropertyValues(location, feature.getProperties(), errors);
            locations.add(location);
        }
        return locations;
    }

    /**
     * Extract expected property values from feature.
     */
    private static void extractPropertyValues(Location location, Map<String, Object> props, List<String> errors) {
        final Set<String> supportedProperties = ImmutableSet.of(STOP_NAME, STOP_DESC, ZONE_ID, STOP_URL);
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String propertyName = entry.getKey().toLowerCase();
            if (supportedProperties.contains(propertyName)) {
                String propertyValue = getPropertyValue(entry.getValue());
                switch (propertyName) {
                    case STOP_NAME:
                        location.stop_name = propertyValue;
                        break;
                    case STOP_DESC:
                        location.stop_desc = propertyValue;
                        break;
                    case ZONE_ID:
                        location.zone_id = propertyValue;
                        break;
                    case STOP_URL:
                        try {
                            location.stop_url = new URL(propertyValue);
                        } catch (MalformedURLException e) {
                            logErrorMessage(
                                String.format("Malformed URL %s.", propertyValue),
                                errors
                            );
                            location.stop_url = null;
                        }
                        break;
                    default:
                        logErrorMessage(
                            String.format("Supported property %s not unpacked!", propertyName),
                            errors
                        );
                        break;
                }
            } else {
                LOG.warn("Unsupported property {}.", propertyName);
            }
        }
    }

    private static void logErrorMessage(String message, List<String> errors) {
        LOG.warn(message);
        if (errors != null) errors.add(message);
    }

    /**
     * Extract the property value from an entry. If the entry value is an integer or double convert to an expected
     * String value.
     */
    private static String getPropertyValue(Object propertyValue) {
        if (propertyValue == null) {
            return null;
        } else if (propertyValue instanceof Integer || propertyValue instanceof Double) {
            return String.valueOf(propertyValue);
        } else {
            return (String) propertyValue;
        }
    }

    /**
     * Extract from a list of features the different geometry types and produce the appropriate {@link LocationShape}
     * representing this geometry type so that enough information is available to revert it back to GeoJson.
     *
     * GeoJson format reference: https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.4
     */
    private static List<LocationShape> unpackLocationShapes(
        FeatureCollection featureCollection,
        List<String> errors
    ) {
        ArrayList<LocationShape> locationShapes = new ArrayList<>();
        Set<String> seenLocationIds = new HashSet<>();
        for (Feature feature : featureCollection.getFeatures()) {
            Geometry geometry = feature.getFeature().getGeometry();
            String geometryType = getGeometryType(geometry.getGeometryType().getName(), errors);
            String locationId = feature.getId();
            if (!isValidLocation(locationId, geometryType, seenLocationIds)) continue;
            switch (geometryType) {
                case GEOMETRY_TYPE_POLYLINE:
                    LineString lineString = (LineString) geometry;
                    for (Point point : lineString.getPoints()) {
                        locationShapes.add(
                            // Because we're only supporting a single linestring right now, use location_id as the geometry_id too
                            buildLocationShape(locationId, locationId, point.getY(), point.getX())
                        );
                    }
                    break;
                case GEOMETRY_TYPE_POLYGON:
                    Polygon polygon = (Polygon) geometry;
                    List<LineString> lineStrings = polygon.getRings();
                    if (lineStrings.size() > 1) {
                        logErrorMessage(
                            "Polygon has multiple line strings, this is not supported yet. Only the " +
                                "first line string will be processed.",
                            errors
                        );
                    }
                    LineString firstLineString = lineStrings.get(0);
                    for (Point point : firstLineString.getPoints()) {
                        locationShapes.add(
                            // Because we're only supporting a single linestring right now, use location_id as the geometry_id too
                            buildLocationShape(feature.getId(), feature.getId(), point.getY(), point.getX())
                        );
                    }
                    break;
                // TODO: Add additional geometry types.
                default:
                    logErrorMessage(
                        String.format(UNSUPPORTED_GEOMETRY_TYPE_MESSAGE, geometryType),
                        errors
                    );
            }
        }
        return locationShapes;
    }

    /**
     * Produces a {@link LocationShape} based on the values provided.
     */
    private static LocationShape buildLocationShape(
        String locationId,
        String geometryId,
        double geometryPtLat,
        double geometryPtLon
    ) {
        LocationShape shape = new LocationShape();
        shape.location_id = locationId;
        shape.geometry_id = geometryId;
        shape.geometry_pt_lat = geometryPtLat;
        shape.geometry_pt_lon = geometryPtLon;
        return shape;
    }

    /**
     * Extract the location features from file, unpack and convert to a CSV representation.
     */
    public static CsvReader getCsvReaderFromGeoJson(
        String tableName,
        ZipFile zipFile,
        ZipEntry entry,
        List<String> errors
    ) {
        FeatureCollection features = GeoJsonUtil.getLocations(zipFile, entry);
        if (features == null || features.numFeatures() == 0) {
            String message = "Unable to extract GeoJson features (or none are available) from " + entry.getName();
            LOG.warn(message);
            if (errors != null) errors.add(message);
            return null;
        }
        StringBuilder csvContent = new StringBuilder();
        if (tableName.equals(Table.LOCATIONS.name)) {
            List<Location> locations = GeoJsonUtil.unpackLocations(features, errors);
            csvContent.append(Location.header());
            locations.forEach(location -> csvContent.append(location.toCsvRow()));
        } else if (tableName.equals(Table.LOCATION_SHAPES.name)) {
            List<LocationShape> locationShapes = GeoJsonUtil.unpackLocationShapes(features, errors);
            csvContent.append(LocationShape.header());
            locationShapes.forEach(locationShape -> csvContent.append(locationShape.toCsvRow()));
        }
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Convert {@link Location} and {@link LocationShape} lists to a serialized String conforming to the GeoJson
     * standard.
     */
    public static String packLocations(
        List<Location> locations,
        List<LocationShape> locationShapes,
        List<String> errors
    ) {
        FeatureCollection featureCollection = new FeatureCollection();
        List<Feature> features = new ArrayList<>();
        for (Location location : locations) {
            switch (location.geometry_type) {
                // location geometry type (polyline) rather than spec LINESTRING
                case GEOMETRY_TYPE_POLYLINE:
                    LineString ls = buildLineString(getLineStings(location.location_id, locationShapes));
                    LineString lineString = new LineString();
                    lineString.setPoints(ls.getPoints());
                    Feature lineStringFeature = new Feature();
                    lineStringFeature.setGeometry(new mil.nga.sf.geojson.LineString(lineString));
                    setFeatureProps(location, lineStringFeature);
                    features.add(lineStringFeature);
                    break;
                case GEOMETRY_TYPE_POLYGON:
                    // We are only supporting a polygon with a single line string so this ok for now. If multiple line
                    // strings are to be supported this will need to change.
                    Polygon polygon = buildPolygon(getLineStings(location.location_id, locationShapes));
                    Feature polygonFeature = new Feature();
                    polygonFeature.setGeometry(new mil.nga.sf.geojson.Polygon(polygon));
                    setFeatureProps(location, polygonFeature);
                    features.add(polygonFeature);
                    break;
                // TODO: Add additional geometry types.
                default:
                    // Effectively, MultiPolygon and MultiLineString types which aren't supported yet.
                    String message = String.format(UNSUPPORTED_GEOMETRY_TYPE_MESSAGE, location.geometry_type);
                    LOG.warn(message);
                    if (errors != null) errors.add(message);
            }
        }
        featureCollection.setFeatures(features);
        return FeatureConverter.toStringValue(featureCollection);
    }

    /**
     * Set the feature id and properties value based on the values held in {@link Location}.
     */
    private static void setFeatureProps(Location location, Feature feature) {
        feature.setId(location.location_id);
        Map<String, Object> properties = new HashMap<>();
        if (location.stop_name != null) properties.put(STOP_NAME, location.stop_name);
        if (location.stop_desc != null) properties.put(STOP_DESC, location.stop_desc);
        if (location.zone_id != null) properties.put(ZONE_ID, location.zone_id);
        if (location.stop_url != null) properties.put(STOP_URL, location.stop_url);
        feature.setProperties(properties);
    }

    /**
     * Produce a single {@link Polygon} from multiple {@link LocationShape} entries.
     */
    private static Polygon buildPolygon(List<LocationShape> polygons) {
        Polygon polygon = new Polygon();
        List<LineString> lineStrings = new ArrayList<>();
        lineStrings.add(buildLineString(polygons));
        polygon.setRings(lineStrings);
        return polygon;
    }

    /**
     * Produce a single {@link LineString} from multiple {@link LocationShape} entries.
     */
    private static LineString buildLineString(List<LocationShape> rings) {
        List<Point> points = buildPoints(rings);
        LineString lineString = new LineString();
        lineString.setPoints(points);
        return lineString;
    }

    /**
     * Produce a list of {@link Point}s from multiple {@link LocationShape} entries.
     */
    private static List<Point> buildPoints(List<LocationShape> locationShapes) {
        List<Point> points = new ArrayList<>();
        locationShapes.forEach(locationShape -> points.add(new Point(locationShape.geometry_pt_lon, locationShape.geometry_pt_lat)));
        return points;
    }

    /**
     * From the provided list of {@link LocationShape}s extract all line string entries and group by line string id.
     */
    private static List<LocationShape> getLineStings(
        String locationId,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.location_id.equals(locationId)
            )
            .collect(toList());
    }
}
