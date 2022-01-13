package com.conveyal.gtfs.util;

import com.conveyal.gtfs.error.NewGTFSError;
import com.conveyal.gtfs.error.SQLErrorStorage;
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
import mil.nga.sf.geojson.MultiLineString;
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
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.conveyal.gtfs.error.NewGTFSErrorType.GEO_JSON_PARSING;
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
     * If a particular reference id is not required when unpacking locations this value is used as a placeholder. This
     * is then referenced when packing locations to focus in on a particular geometry type.
     */
    private static final int NOT_REQUIRED = -1;

    private static final String PROP_KEY_VALUE_SEPARATOR = "~";
    private static final String PROP_SEPARATOR = "#";

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
     * Extract from a list of features, the items which are common to all features.
     */
    private static List<Location> unpackLocations(FeatureCollection featureCollection) throws MalformedURLException {
        ArrayList<Location> locations = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        for (Feature feature : features) {
            Location location = new Location();
            location.location_id = feature.getId();
            String geometry_type = feature.getGeometryType().getName();
            // The front end uses (for now) geometry types defined by react-leaflet-draw
            // rather than those explicitly in the flex spec.
            // Here we'll just convert the flex spec types to those used by the front end
            switch (geometry_type) {
                case "MULTILINESTRING":
                    location.geometry_type = "multipolyline";
                    break;
                case "LINESTRING":
                    location.geometry_type = "polyline";
                    break;
                default:
                    // Effectively, POLYGON and MULTIPOLYGON types which aren't supported yet.
                    continue;
//                    location.geometry_type = geometry_type.toLowerCase();
//                    break;
            }
            Map<String, Object> props = feature.getProperties();
            // To avoid any comma related issues when reading this data in, the PROP_KEY_VALUE_SEPARATOR
            // and PROP_SEPARATOR characters are used.

            for (Map.Entry<String, Object> entry : props.entrySet()) {
                if (entry.getValue() != null) {
                    switch (entry.getKey()) {
                        case "stop_name":
                            location.stop_name = (String) entry.getValue();
                            break;
                        case "stop_desc":
                            location.stop_desc = (String) entry.getValue();
                            break;
                        case "zone_id":
                            location.zone_id = (String) entry.getValue();
                            break;
                        case "stop_url":
                            location.stop_url = new URL((String) entry.getValue());
                            break;
                        default:
                            //TODO: handle invalid GTFS
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
        FeatureCollection featureCollection
    ) {
        ArrayList<LocationShape> locationShapes = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        int ringId;
        int sequenceId;
        int lineId;
        for (Feature feature : features) {
            Geometry geometry = feature.getFeature().getGeometry();
            switch(geometry.getGeometryType()) {
//                case MULTIPOLYGON:
//                    MultiPolygon multiPolygon = (MultiPolygon) geometry;
//                    List<Polygon> polygons = multiPolygon.getPolygons();
//                    int polygonId = 0;
//                    for (Polygon polygon : polygons) {
//                        polygonId++;
//                        ringId = 0;
//                        for (LineString lineString : polygon.getRings()) {
//                            List<Point> points = lineString.getPoints();
//                            ringId++;
//                            sequenceId = 1;
//                            for (Point point : points) {
//                                locationShapes.add(
//                                    // TODO: build these properties correctly before passing.
//                                    buildLocationShape("testId1", "G", "Polygon", 43.649528,-79.462676)
//                                );
//                            }
//                        }
//                    }
//                    break;
                case POLYGON:
                    Polygon polygon = (Polygon) geometry;
                    ringId = 0;
                    // Assumption, the first ring is the exterior ring and all subsequent rings are interior.
                    // TODO: handle rings once the front end is ready for it, since it may require saving ring IDs
                    // For now, we'll rly only be expecting 1 linestring from polygon.getRings
                    for (LineString lineString : polygon.getRings()) {
                        List<Point> points = lineString.getPoints();
                        ringId++;
                        for (Point point : points) {
                            locationShapes.add(
                                // Use the ringId as the geometryID since w/ the assumption of max rings = 1, this is the same.
                                buildLocationShape(feature.getId(), Integer.toString(ringId), point.getY(), point.getX())
                            );
                        }
                    }
                    break;
//                case MULTILINESTRING:
//                    MultiLineString multiLineString = (MultiLineString) geometry;
//                    List<LineString> lineStrings = multiLineString.getLineStrings();
//                    lineId = 0;
//                    for(LineString lineString : lineStrings) {
//                        List<Point> points = lineString.getPoints();
//                        sequenceId = 1;
//                        lineId++;
//                        for (Point point : points) {
//                            locationShapes.add(
//                                    // TODO: build these properties correctly before passing.
//                                    buildLocationShape("testId1", "G", "Polygon", 43.649528,-79.462676)
//                            );
////                            locationShapes.add(buildLocationShape(shapeId++, point, lineId, sequenceId++, feature.getId()));
//                        }
//                    }
//                    break;
                case LINESTRING:
                    LineString lineString = (LineString) geometry;
                    List<Point> points = lineString.getPoints();
                    for (Point point : points) {
                        locationShapes.add(
                            // Because we're only supporting a single linestring right now, use location_id as the geometry_id too
                            buildLocationShape(feature.getId(), feature.getId(), point.getY(), point.getX())
                        );
                    }
                    break;
                default:
                    String message = String.format("Geometry type %s unknown or not supported.", geometry.getGeometryType());
                    LOG.warn(message);
//                    if (sqlErrorStorage != null) sqlErrorStorage.storeError(NewGTFSError.forFeed(GEO_JSON_PARSING, message));
            }
        }
        return locationShapes;
    }

    /**
     * Used to produce a location shape representing a multi line string and line string.
     */
//    private static LocationShape buildLocationShape(int shapeId, Point point, int lineStringId, int sequenceId, String locationMetaDataId) {
//        return buildLocationShape(shapeId, NOT_REQUIRED, NOT_REQUIRED, point, lineStringId, sequenceId, locationMetaDataId);
//    }

    /**
     * Used to produce a location shape representing a polygon.
     */
//    private static LocationShape buildLocationShape(int shapeId, int ringId, Point point, int sequenceId, String locationMetaDataId) {
//        return buildLocationShape(shapeId, NOT_REQUIRED, ringId, point, NOT_REQUIRED, sequenceId, locationMetaDataId);
//    }

    /**
     * Used to produce a location shape representing a multi-polygon.
     */
//    private static LocationShape buildLocationShape(int shapeId, int polygonId, int ringId, Point point, int sequenceId, String locationMetaDataId) {
//        return buildLocationShape();
//    }

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
        ZipEntry entry
    ) {
        FeatureCollection features = GeoJsonUtil.getLocations(zipFile, entry);
        if (features == null || features.numFeatures() == 0) {
            String message = "Unable to extract GeoJson features (or none are available) from " +  entry.getName();
            LOG.warn(message);
//            if (sqlErrorStorage != null) sqlErrorStorage.storeError(NewGTFSError.forFeed(GEO_JSON_PARSING, message));
            return null;
        }
        StringBuilder csvContent = new StringBuilder();
        if (tableName.equals(Table.LOCATIONS.name)) {
            List<Location> locations = null;
            try {
                locations = GeoJsonUtil.unpackLocations(features);
            } catch (MalformedURLException e) {
                // TODO: Handle this error properly
                e.printStackTrace();
            }
            csvContent.append(Location.header());
            locations.forEach(location -> csvContent.append(location.toCsvRow()));
        } else if (tableName.equals(Table.LOCATION_SHAPES.name)) {
            List<LocationShape> locationShapes = GeoJsonUtil.unpackLocationShapes(features);
            csvContent.append(LocationShape.header());
            locationShapes.forEach(locationShape -> csvContent.append(locationShape.toCsvRow()));
        }
        return new CsvReader(new StringReader(csvContent.toString()));
    }

    /**
     * Convert {@link Location} and {@link LocationShape} lists to a serialized String conforming to the GeoJson
     * standard.
     */
    public static String packLocations(List<Location> locations, List<LocationShape> locationShapes)
        throws GeoJsonException {

        FeatureCollection featureCollection = new FeatureCollection();
        List<Feature> features = new ArrayList<>();
        for (Location location : locations) {
            switch (location.geometry_type) {
//                case "MULTIPOLYGON":
//                    List<Polygon> polygons = new ArrayList<>();
//                    Map<String, List<LocationShape>> polygonsForMetaData = getMultiPolygons(meta.location_meta_data_id, locationShapes);
//                    polygonsForMetaData.forEach((polygonId, multiPolygons) -> {
//                        polygons.add(buildPolygon(multiPolygons));
//                    });
//                    MultiPolygon multiPolygon = new MultiPolygon();
//                    multiPolygon.setPolygons(polygons);
//                    mil.nga.sf.geojson.Feature multiPolygonFeature = new mil.nga.sf.geojson.Feature();
//                    multiPolygonFeature.setGeometry(new mil.nga.sf.geojson.MultiPolygon(multiPolygon));
//                    setFeatureProps(meta, multiPolygonFeature);
//                    features.add(multiPolygonFeature);
//                    break;
                case "polygon":
                    List<LocationShape> polygons = getPolygons(location.location_id, locationShapes);
                    Polygon polygon = buildPolygon(polygons);
                    mil.nga.sf.geojson.Feature polygonFeature = new mil.nga.sf.geojson.Feature();
                    polygonFeature.setGeometry(new mil.nga.sf.geojson.Polygon(polygon));
                    setFeatureProps(location, polygonFeature);
                    features.add(polygonFeature);
                    break;
                // location geometry type (multipolyline) rather than spec MULTILINESTRING
//                case "multipolyline":
//                    List<LineString> lineStrings = new ArrayList<>();
//                    Map<Integer, List<LocationShape>> multiLineStrings = getMultiLineStings(location.location_id, locationShapes);
//                    multiLineStrings.forEach((lineStringId, lineString) -> {
//                        lineStrings.add(buildLineString(lineString));
//                    });
//                    MultiLineString multiLineString = new MultiLineString();
//                    multiLineString.setLineStrings(lineStrings);
//                    mil.nga.sf.geojson.Feature multiLineStringFeature = new mil.nga.sf.geojson.Feature();
//                    multiLineStringFeature.setGeometry(new mil.nga.sf.geojson.MultiLineString(multiLineString));
//                    setFeatureProps(location, multiLineStringFeature);
//                    features.add(multiLineStringFeature);
//                    break;
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
                default:
                    String message = String.format("Geometry type %s unknown or not supported.", location.geometry_type);
                    LOG.warn(message);
//                    throw new GeoJsonException(message);
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

        //TODO: we'll need something like this when we're working w/ multipolygons
//        polygons.forEach(locationShape -> {
//            lineStrings.add(buildLineString(locationShape));
//        });
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
     * From the provided list of {@link LocationShape}s group by {@link LocationShape#geometry_id}.
     */
    private static Map<String, List<LocationShape>> getPolygons(
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .collect(
                Collectors.groupingBy(
                    LocationShape::getGeometry_id,
                    Collectors.mapping((LocationShape l) -> l, toList())
                )
            );
    }

    /**
     * From the provided list of {@link LocationShape}s extract all polygon entries and group by polygon id.
     */
    private static Map<String, List<LocationShape>> getMultiPolygons(
        String location_id,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.location_id.equals(location_id)
            )
            .sorted(
                Comparator
                    .comparing(LocationShape::getGeometry_id)
            )
            .collect(
                Collectors.groupingBy(
                    LocationShape::getGeometry_id,
                    Collectors.mapping((LocationShape l) -> l, toList())
                )
            );
    }

    /**
     * From the provided list of {@link LocationShape}s extract all rings entries.
     */
    private static List<LocationShape> getPolygons(
        String location_id,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.location_id.equals(location_id)
            )
            .sorted(
                Comparator
                    .comparing(LocationShape::getGeometry_id)
            )
            .collect(toList());
    }

    /**
     * From the provided list of {@link LocationShape}s extract all line string entries and group by line string id.
     */
    // TODO: adapt this once frontend is ready for multipolygons
//    private static Map<Integer, List<LocationShape>> getMultiLineStings(
//        String locationMetaDataId,
//        List<LocationShape> locationShapes
//    ) {
//        return locationShapes
//            .stream()
//            .filter(
//                item -> item.shape_polygon_id == NOT_REQUIRED &&
//                item.shape_ring_id == NOT_REQUIRED &&
//                item.shape_line_string_id != NOT_REQUIRED &&
//                item.location_meta_data_id.equals(locationMetaDataId)
//            )
//            .sorted(
//                Comparator
//                    .comparing(LocationShape::getShape_line_string_id)
//                    .thenComparing(LocationShape::getShape_pt_sequence)
//            )
//            .collect(
//                Collectors.groupingBy(
//                    LocationShape::getShape_line_string_id,
//                    Collectors.mapping((LocationShape l) -> l, toList())
//                )
//            );
//    }

    /**
     * From the provided list of {@link LocationShape}s extract all line string entries and group by line string id.
     */
    // TODO: determine if sorting is necessary (i.e. previously we were doing .thenComparing(LocationShape::getShape_pt_sequence) )
    private static List<LocationShape> getLineStings(
        String location_id,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.location_id.equals(location_id)
            )
            .sorted(
                Comparator
                    .comparing(LocationShape::getGeometry_id)
            )
            .collect(toList());
    }
}
