package com.conveyal.gtfs.util.json;

import com.conveyal.geojson.GeoJsonModule;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;

/**
 * Helper methods for writing REST API routines
 * @author mattwigway
 *
 */
public class JsonManager<T> {
    private ObjectWriter ow;
    private ObjectMapper om;

    /**
     * Create a new JsonManager
     * @param theClass The class to create a json manager for (yes, also in the diamonds).
     * @param view The view to use
     */
    public JsonManager (Class<T> theClass) {
        this.theClass = theClass;
        this.om = new ObjectMapper();
        om.addMixInAnnotations(Rectangle2D.class, Rectangle2DMixIn.class);
        om.registerModule(new GeoJsonModule());
        SimpleModule deser = new SimpleModule();

        deser.addDeserializer(LocalDate.class, new JacksonSerializers.LocalDateStringDeserializer());
        deser.addSerializer(LocalDate.class, new JacksonSerializers.LocalDateStringSerializer());

        deser.addDeserializer(Rectangle2D.class, new Rectangle2DDeserializer());
        om.registerModule(deser);
        om.getSerializerProvider().setNullKeySerializer(new JacksonSerializers.MyDtoNullKeySerializer());
        SimpleFilterProvider filters = new SimpleFilterProvider();
        filters.addFilter("bbox", SimpleBeanPropertyFilter.filterOutAllExcept("west", "east", "south", "north"));
        this.ow = om.writer(filters);
    }

    private Class<T> theClass;

    /**
     * Add an additional mixin for serialization with this object mapper.
     */
    public void addMixin(Class target, Class mixin) {
        om.addMixInAnnotations(target, mixin);
    }

    public String write(Object o) throws JsonProcessingException {
        if (o instanceof String) {
            return (String) o;
        }
        return ow.writeValueAsString(o);
    }

    /**
     * Convert an object to its JSON representation
     * @param o the object to convert
     * @return the JSON string
     * @throws JsonProcessingException
     */
    /*public String write (T o) throws JsonProcessingException {
        return ow.writeValueAsString(o);
    }*/

    /**
     * Convert a collection of objects to their JSON representation.
     * @param c the collection
     * @return A JsonNode representing the collection
     * @throws JsonProcessingException
     */
    public String write (Collection<T> c) throws JsonProcessingException {
        return ow.writeValueAsString(c);
    }

    public String write (Map<String, T> map) throws JsonProcessingException {
        return ow.writeValueAsString(map);
    }

    public T read (String s) throws JsonParseException, JsonMappingException, IOException {
        return om.readValue(s, theClass);
    }

    public T read (JsonParser p) throws JsonParseException, JsonMappingException, IOException {
        return om.readValue(p, theClass);
    }

    public T read(JsonNode asJson) {
        return om.convertValue(asJson, theClass);
    }
}
