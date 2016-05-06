package com.conveyal.gtfs.validator;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.error.IllegalShapeError;
import com.conveyal.gtfs.model.Shape;
import com.conveyal.gtfs.validator.model.InvalidValue;
import com.conveyal.gtfs.validator.model.Priority;
import com.conveyal.gtfs.validator.model.ValidationResult;
import com.conveyal.gtfs.validator.service.GeoUtils;
import com.conveyal.gtfs.validator.service.ProjectedCoordinate;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.mapdb.Fun;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by landon on 5/2/16.
 */
public class IllegalShapeValidator extends GTFSValidator {

    static GeometryFactory geometryFactory = new GeometryFactory();

    @Override
    public boolean validate(GTFSFeed feed, boolean repair) {
        boolean isValid = true;
        ValidationResult result = new ValidationResult();

        // create geometries from shapePoints
        HashMap<String, Geometry> shapes = new HashMap<String, Geometry>();

        for(Map.Entry<String, Map<Integer, Shape>> entry : feed.shapes.entrySet()) {
            String shapeId = entry.getKey();
            Map<Integer, Shape> shapePoints = entry.getValue();

            ArrayList<Coordinate> shapeCoords = new ArrayList<Coordinate>();

            for(Shape shapePoint : shapePoints.values()) {

                Coordinate stopCoord = new Coordinate(shapePoint.shape_pt_lat, shapePoint.shape_pt_lon);

                try {
                    ProjectedCoordinate projectedStopCoord = GeoUtils.convertLatLonToEuclidean(stopCoord);
                    shapeCoords.add(projectedStopCoord);
                } catch (Exception e) {
                    isValid = false;
                    feed.errors.add(new IllegalShapeError("stop", 0, "shapeId", shapeId, Priority.MEDIUM));
//                    result.add(new InvalidValue("stop", "shapeId", shapeId, "Illegal stopCoord for shape", "", null, Priority.MEDIUM));
                }
            }

            Geometry geom = geometryFactory.createLineString(shapeCoords.toArray(new Coordinate[shapePoints.size()]));

            shapes.put(shapeId, geom);

        }

        return isValid;
    }
}
