package com.conveyal.gtfs.util;

import com.conveyal.gtfs.model.LocationMetaData;
import com.conveyal.gtfs.model.LocationShape;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

// https://ngageoint.github.io/simple-features-geojson-java/
public class GeoJsonUtil {

    private static final Logger LOG = LoggerFactory.getLogger(GeoJsonUtil.class);

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

    public static List<LocationMetaData> getLocationMetaData(FeatureCollection featureCollection) {
        ArrayList<LocationMetaData> locationMetaData = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        for (Feature feature : features) {
            LocationMetaData location = new LocationMetaData();
            location.location_meta_data_id = feature.getId();
            location.geometry_type = feature.getGeometryType().getName();
            Map<String, Object> props = feature.getProperties();
            location.properties = props.keySet().stream()
                .map(key -> key + "~" + props.get(key))
                .collect(Collectors.joining("|"));
            locationMetaData.add(location);

        }
        return locationMetaData;
    }

    public static List<LocationShape> getLocationShapes(FeatureCollection featureCollection) {
        ArrayList<LocationShape> locationShapes = new ArrayList<>();
        List<Feature> features = featureCollection.getFeatures();
        int shapeId = 1;
        for (Feature feature : features) {
            Geometry geometry = feature.getFeature().getGeometry();
            if (geometry instanceof Polygon) {
                int sequenceId = 1;
                Polygon polygon = (Polygon) geometry;
                LineString lineStrings = polygon.getRings().get(0);
                List<Point> points = lineStrings.getPoints();
                for (Point point : points) {
                    LocationShape shape = new LocationShape();
                    shape.shape_id = Integer.toString(shapeId++);
                    shape.shape_pt_lat = point.getX();
                    shape.shape_pt_lon = point.getY();
                    shape.shape_pt_sequence = sequenceId++;
                    shape.location_meta_data_id = feature.getId();
                    locationShapes.add(shape);
                }
            } else {
                //TODO: Add other geometry types.
                return null;
            }

        }
        return locationShapes;
    }

    public static String toGeoJson(FeatureCollection featureCollection) {
        return FeatureConverter.toStringValue(featureCollection);
    }

//    public static void main(String[] args) throws IOException {
//        ZipFile zipFile = new ZipFile("C:\\projects\\JetBrains\\OTP\\gtfs-lib\\src\\test\\resources\\real-world-gtfs-feeds\\gtfs_GL.zip");
//        FeatureCollection features = getLocations(zipFile);
//        assert features != null;
//        System.out.println(features.numFeatures());
//        List<LocationMetaData> locationMetaData = getLocationMetaData(features);
//        locationMetaData.forEach(System.out::println);
//        List<LocationShape> locationShapes = getLocationShapes(features);
//        locationShapes.forEach(System.out::println);
//        Reader reader = new StringReader("\"location_meta_data_id\", \"properties\", \"geometry_type\"\nv1,v2,v3");
//        CsvReader csvReader = new CsvReader(reader);
//        csvReader.setSkipEmptyRecords(false);
//        csvReader.readHeaders();
//        while (csvReader.readRecord()) {
//            System.out.println(csvReader.getColumnCount());
//            for (int f = 0; f < 3; f++) {
//                System.out.println(csvReader.get(f));
//            }
//        }
//    }
}
