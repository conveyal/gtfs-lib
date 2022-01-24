package com.conveyal.gtfs.util;

import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationShape;
import com.csvreader.CsvReader;
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
import java.util.List;
import java.util.Map;
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

    /**
     * Takes the content of a zip file entry and converts it into a {@link FeatureCollection} which is a class
     * representation of features held in the locations file.
     */
    private static FeatureCollection getLocations(ZipFile zipFile, ZipEntry entry) {
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
    private static String getGeometryType(String geometry_type, List<String> errors) {
        switch (geometry_type) {
            case "LINESTRING":
                return "polyline";
            case "POLYGON":
                return "polygon";
            // TODO: Add additional geometry types.
            default:
                // Effectively, MultiPolygon, Polygon and MultiLineString types which aren't supported yet.
                String message = String.format("Geometry type %s unknown or not supported.", geometry_type);
                LOG.warn(message);
                if (errors != null) errors.add(message);
                return null;
        }
    }

    /**
     * Extract from a list of features, the items which are common to all features.
     */
    private static List<Location> unpackLocations(
        FeatureCollection featureCollection,
        List<String> errors
    ) {
        ArrayList<Location> locations = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        for (Feature feature : features) {
            String geometryType = getGeometryType(feature.getGeometryType().getName(), errors);
            if (geometryType == null) continue;
            Location location = new Location();
            location.location_id = feature.getId();
            location.geometry_type = geometryType;
            Map<String, Object> props = feature.getProperties();
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                if (entry.getValue() != null) {
                    String property = (String) entry.getValue();
                    switch (entry.getKey()) {
                        case "stop_name":
                            location.stop_name = property;
                            break;
                        case "stop_desc":
                            location.stop_desc = property;
                            break;
                        case "zone_id":
                            location.zone_id = property;
                            break;
                        case "stop_url":
                            try {
                                location.stop_url = new URL(property);
                            } catch (MalformedURLException e) {
                                String message = String.format("Malformed URL %s.", property);
                                LOG.warn(message);
                                if (errors != null) errors.add(message);
                                location.stop_url = null;
                            }
                            break;
                        default:
                            String message = String.format("Unsupported property %s.", property);
                            LOG.warn(message);
                            if (errors != null) errors.add(message);
                            break;
                    }
                }
            }
            locations.add(location);
        }
        return locations;
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
        List<Feature> features = featureCollection.getFeatures();
        for (Feature feature : features) {
            Geometry geometry = feature.getFeature().getGeometry();
            String geometryType = getGeometryType(geometry.getGeometryType().getName(), errors);
            if (geometryType == null) continue;
            switch (geometryType) {
                case "polyline":
                    LineString lineString = (LineString) geometry;
                    List<Point> points = lineString.getPoints();
                    for (Point point : points) {
                        locationShapes.add(
                            // Because we're only supporting a single linestring right now, use location_id as the geometry_id too
                            buildLocationShape(feature.getId(), feature.getId(), point.getY(), point.getX())
                        );
                    }
                    break;
                case "polygon":
                    Polygon polygon = (Polygon) geometry;
                    List<LineString> lineStrings = polygon.getRings();
                    if (lineStrings.size() > 1) {
                        String message = "Polygon has multiple line strings, this is not supported yet. Only the " +
                            "first line string will be processed.";
                        LOG.warn(message);
                        if (errors != null) errors.add(message);
                    }
                    LineString firstLineString = lineStrings.get(0);
                    List<Point> p = firstLineString.getPoints();
                    for (Point point : p) {
                        locationShapes.add(
                            // Because we're only supporting a single linestring right now, use location_id as the geometry_id too
                            buildLocationShape(feature.getId(), feature.getId(), point.getY(), point.getX())
                        );
                    }

                    break;
                // TODO: Add additional geometry types.
            }
        }
        return locationShapes;
    }

    /**
     * Produces a {@link LocationShape} based on the values provided.
     */
    private static LocationShape buildLocationShape(
        String location_id,
        String geometry_id,
        double geometry_pt_lat,
        double geometry_pt_lon
    ) {
        LocationShape shape = new LocationShape();
        shape.location_id = location_id;
        shape.geometry_id = geometry_id;
        shape.geometry_pt_lat = geometry_pt_lat;
        shape.geometry_pt_lon = geometry_pt_lon;
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
                case "polyline":
                    LineString ls = buildLineString(getLineStings(location.location_id, locationShapes));
                    LineString lineString = new LineString();
                    lineString.setPoints(ls.getPoints());
                    mil.nga.sf.geojson.Feature lineStringFeature = new mil.nga.sf.geojson.Feature();
                    lineStringFeature.setGeometry(new mil.nga.sf.geojson.LineString(lineString));
                    setFeatureProps(location, lineStringFeature);
                    features.add(lineStringFeature);
                    break;
                case "polygon":
                    // We are only supporting a polygon with a single line string so this ok for now. If multiple line
                    // strings are to be supported this will need to change.
                    Polygon polygon = buildPolygon(getLineStings(location.location_id, locationShapes));
                    mil.nga.sf.geojson.Feature polygonFeature = new mil.nga.sf.geojson.Feature();
                    polygonFeature.setGeometry(new mil.nga.sf.geojson.Polygon(polygon));
                    setFeatureProps(location, polygonFeature);
                    features.add(polygonFeature);
                    break;
                // TODO: Add additional geometry types.
                default:
                    // Effectively, MultiPolygon and MultiLineString types which aren't supported yet.
                    String message = String.format("Geometry type %s unknown or not supported.", location.geometry_type);
                    LOG.warn(message);
                    if (errors != null) errors.add(message);
            }
        }
        featureCollection.setFeatures(features);
        LOG.info(FeatureConverter.toStringValue(featureCollection));
        return FeatureConverter.toStringValue(featureCollection);
    }

    /**
     * Set the feature id and properties value based on the values held in {@link Location}.
     */
    private static void setFeatureProps(Location location, Feature feature) {
        feature.setId(location.location_id);
        Map<String, Object> properties = new HashMap<>();
        if (location.stop_name != null) properties.put("stop_name", location.stop_name);
        if (location.stop_desc != null) properties.put("stop_desc", location.stop_desc);
        if (location.zone_id != null) properties.put("zone_id", location.zone_id);
        if (location.stop_url != null) properties.put("stop_url", location.stop_url);
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
        locationShapes.forEach(locationShape -> {
            points.add(new Point(locationShape.geometry_pt_lat, locationShape.geometry_pt_lon));
        });
        return points;
    }

    /**
     * From the provided list of {@link LocationShape}s extract all line string entries and group by line string id.
     */
    private static List<LocationShape> getLineStings(
        String location_id,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.location_id.equals(location_id)
            )
            .collect(toList());
    }
}
