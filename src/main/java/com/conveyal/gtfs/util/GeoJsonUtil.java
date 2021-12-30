package com.conveyal.gtfs.util;

import com.conveyal.gtfs.loader.Table;
import com.conveyal.gtfs.model.Location;
import com.conveyal.gtfs.model.LocationMetaData;
import com.conveyal.gtfs.model.LocationShape;
import com.csvreader.CsvReader;
import mil.nga.sf.Geometry;
import mil.nga.sf.LineString;
import mil.nga.sf.MultiLineString;
import mil.nga.sf.MultiPolygon;
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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
 * {@link LocationMetaData} and {@link LocationShape}. Packing does the opposite by using these two classes to convert
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
    private static List<Location> unpackLocationData(FeatureCollection featureCollection) {
        ArrayList<Location> locations = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        for (Feature feature : features) {
            Location location = new Location();
            location.location_id = feature.getId();
            // location.geometry_type = feature.getGeometryType().getName(); // TODO: fix this so that Location includes a geometry_type field
            Map<String, Object> props = feature.getProperties();
            // To avoid any comma related issues when reading this data in, the PROP_KEY_VALUE_SEPARATOR
            // and PROP_SEPARATOR characters are used.

            for (Map.Entry<String, Object> entry : props.entrySet()) {
                System.out.println(entry.getKey() + "/" + entry.getValue());
            }

//            location.properties = props.keySet().stream()
//                .map(key -> key + PROP_KEY_VALUE_SEPARATOR + props.get(key))
//                .collect(Collectors.joining(PROP_SEPARATOR));
//            locations.add(location);

        }
        return locations;
    }

    /**
     * Extract from a list of features the different geometry types and produce the appropriate {@link LocationShape}
     * representing this geometry type so that enough information is available to revert it back to GeoJson.
     *
     * GeoJson format reference: https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.4
     */
    private static List<LocationShape> unpackLocationShapes(FeatureCollection featureCollection)
        throws GeoJsonException {

        ArrayList<LocationShape> locationShapes = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        int shapeId = 1;
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
                    for (LineString lineString : polygon.getRings()) {
                        List<Point> points = lineString.getPoints();
                        ringId++;
                        sequenceId = 1;
                        for (Point point : points) {
                            locationShapes.add(
                                    // TODO: build these properties correctly before passing.
                                    buildLocationShape("testId1", "G", "Polygon", 43.649528,-79.462676)
                            );
//                            locationShapes.add(buildLocationShape(shapeId++, ringId, point, sequenceId++, feature.getId()));
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
                    sequenceId = 1;
                    lineId = 1;
                    for (Point point : points) {
                        locationShapes.add(
                                // TODO: build these properties correctly before passing.
                                buildLocationShape("testId1", "G", "Polygon", 43.649528,-79.462676)
                        );
//                        locationShapes.add(buildLocationShape(shapeId++, point, lineId, sequenceId++, feature.getId()));
                    }
                    break;
                default:
                    String message = String.format("Geometry type %s unknown or not supported.", geometry.getGeometryType());
                    LOG.warn(message);
                    throw new GeoJsonException(message);
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
        String geometry_type,
        double geometry_pt_lat,
        double geometry_pt_lon
//        int shapeId,
//        int polygonId,
//        int ringId,
//        Point point,
//        int lineStringId,
//        int sequenceId,
//        String locationMetaDataId
    ) {
        LocationShape shape = new LocationShape();
        shape.geometry_id = geometry_id;
        shape.location_id = location_id;
        shape.geometry_type = geometry_type;
        shape.geometry_pt_lat = geometry_pt_lat;
        shape.geometry_pt_lon = geometry_pt_lon;

//        shape.shape_id = Integer.toString(shapeId);
//        shape.shape_polygon_id = polygonId;
//        shape.shape_ring_id = ringId;
//        shape.shape_line_string_id = lineStringId;
//        shape.shape_pt_lat = point.getX();
//        shape.shape_pt_lon = point.getY();
//        shape.shape_pt_sequence = sequenceId;
//        shape.location_meta_data_id = locationMetaDataId;
        return shape;
    }

    /**
     * Extract the location features from file, unpack and convert to a CSV representation.
     */
    public static CsvReader getCsvReaderFromGeoJson(String tableName, ZipFile zipFile, ZipEntry entry)
        throws GeoJsonException {

        FeatureCollection features = GeoJsonUtil.getLocations(zipFile, entry);
        if (features == null || features.numFeatures() == 0) {
            String message = "Unable to extract GeoJson features (or none are available) from " +  entry.getName();
            LOG.warn(message);
            throw new GeoJsonException(message);
        }
        StringBuilder csvContent = new StringBuilder();
        if (tableName.equals(Table.LOCATIONS.name)) {
            List<Location> locations = GeoJsonUtil.unpackLocationData(features);
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
     * Convert {@link LocationMetaData} and {@link LocationShape} lists to a serialized String conforming to the GeoJson
     * standard.
     */
    public static String packLocations(List<LocationMetaData> locationMetaData, List<LocationShape> locationShapes)
        throws GeoJsonException {

        FeatureCollection featureCollection = new FeatureCollection();
        List<Feature> features = new ArrayList<>();
        for (LocationMetaData meta : locationMetaData) {
            switch (meta.geometry_type) {
                case "MULTIPOLYGON":
                    List<Polygon> polygons = new ArrayList<>();
                    Map<Integer, List<LocationShape>> polygonsForMetaData = getMultiPolygons(meta.location_meta_data_id, locationShapes);
                    polygonsForMetaData.forEach((polygonId, multiPolygons) -> {
                        polygons.add(buildPolygon(multiPolygons));
                    });
                    MultiPolygon multiPolygon = new MultiPolygon();
                    multiPolygon.setPolygons(polygons);
                    mil.nga.sf.geojson.Feature multiPolygonFeature = new mil.nga.sf.geojson.Feature();
                    multiPolygonFeature.setGeometry(new mil.nga.sf.geojson.MultiPolygon(multiPolygon));
                    setFeatureProps(meta, multiPolygonFeature);
                    features.add(multiPolygonFeature);
                    break;
                case "POLYGON":
                    List<LocationShape> polygonRings = getPolygons(meta.location_meta_data_id, locationShapes);
//                  List<LocationShape> polygonRings = getPolygonRings(meta.location_meta_data_id, locationShapes);
                    Polygon polygon = buildPolygon(polygonRings);
                    mil.nga.sf.geojson.Feature polygonFeature = new mil.nga.sf.geojson.Feature();
                    polygonFeature.setGeometry(new mil.nga.sf.geojson.Polygon(polygon));
                    setFeatureProps(meta, polygonFeature);
                    features.add(polygonFeature);
                    break;
                case "MULTILINESTRING":
                    List<LineString> lineStrings = new ArrayList<>();
                    Map<Integer, List<LocationShape>> multiLineStrings = getMultiLineStings(meta.location_meta_data_id, locationShapes);
                    multiLineStrings.forEach((lineStringId, lineString) -> {
                        lineStrings.add(buildLineString(lineString));
                    });
                    MultiLineString multiLineString = new MultiLineString();
                    multiLineString.setLineStrings(lineStrings);
                    mil.nga.sf.geojson.Feature multiLineStringFeature = new mil.nga.sf.geojson.Feature();
                    multiLineStringFeature.setGeometry(new mil.nga.sf.geojson.MultiLineString(multiLineString));
                    setFeatureProps(meta, multiLineStringFeature);
                    features.add(multiLineStringFeature);
                    break;
                case "LINESTRING":
                    LineString ls = buildLineString(getLineStings(meta.location_meta_data_id, locationShapes));
                    LineString lineString = new LineString();
                    lineString.setPoints(ls.getPoints());
                    mil.nga.sf.geojson.Feature lineStringFeature = new mil.nga.sf.geojson.Feature();
                    lineStringFeature.setGeometry(new mil.nga.sf.geojson.LineString(lineString));
                    setFeatureProps(meta, lineStringFeature);
                    features.add(lineStringFeature);
                    break;
                default:
                    String message = String.format("Geometry type %s unknown or not supported.", meta.geometry_type);
                    LOG.warn(message);
                    throw new GeoJsonException(message);
            }
        }
        featureCollection.setFeatures(features);
        return FeatureConverter.toStringValue(featureCollection);
    }

    /**
     * Set the feature id and properties value based on the values held in {@link LocationMetaData}.
     */
    private static void setFeatureProps(LocationMetaData metaData, Feature feature) {
        feature.setId(metaData.location_meta_data_id);
        if (!metaData.properties.isEmpty()) {
            String[] props = metaData.properties.split(PROP_SEPARATOR);
            Map<String, Object> properties = new HashMap<>();
            Arrays.stream(props).forEach(prop -> {
                String key = prop.split(PROP_KEY_VALUE_SEPARATOR)[0];
                String value = prop.split(PROP_KEY_VALUE_SEPARATOR)[1];
                properties.put(key, value);
            });
            feature.setProperties(properties);
        }
    }

    /**
     * Produce a single {@link Polygon} from multiple {@link LocationShape} entries.
     */
    private static Polygon buildPolygon(List<LocationShape> polygons) {
        Polygon polygon = new Polygon();
        List<LineString> lineStrings = new ArrayList<>();
        Map<Integer, List<LocationShape>> ringsForPolygons = getPolygonRings(polygons);
        ringsForPolygons.forEach((ringId, rings) -> {
            lineStrings.add(buildLineString(rings));
        });
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
    private static Map<Integer, List<LocationShape>> getMultiLineStings(
        String locationMetaDataId,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.shape_polygon_id == NOT_REQUIRED &&
                item.shape_ring_id == NOT_REQUIRED &&
                item.shape_line_string_id != NOT_REQUIRED &&
                item.location_meta_data_id.equals(locationMetaDataId)
            )
            .sorted(
                Comparator
                    .comparing(LocationShape::getShape_line_string_id)
                    .thenComparing(LocationShape::getShape_pt_sequence)
            )
            .collect(
                Collectors.groupingBy(
                    LocationShape::getShape_line_string_id,
                    Collectors.mapping((LocationShape l) -> l, toList())
                )
            );
    }

    /**
     * From the provided list of {@link LocationShape}s extract all line string entries and group by line string id.
     */
    private static List<LocationShape> getLineStings(
        String locationMetaDataId,
        List<LocationShape> locationShapes
    ) {
        return locationShapes
            .stream()
            .filter(
                item -> item.shape_polygon_id == NOT_REQUIRED &&
                    item.shape_ring_id == NOT_REQUIRED &&
                    item.shape_line_string_id != NOT_REQUIRED &&
                    item.location_meta_data_id.equals(locationMetaDataId)
            )
            .sorted(
                Comparator
                    .comparing(LocationShape::getShape_line_string_id)
                    .thenComparing(LocationShape::getShape_pt_sequence)
            )
            .collect(toList());
    }
}
