package com.conveyal.gtfs.model;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A map of a single shape_id with points indexed by shape_point_sequence.
 * Backed by a submap, but eliminates the need to refer to shape points always by shape ID.
 * @author mattwigway
 */
public class ShapeMap implements Map<Integer, Shape> {
    private String shapeId;
    
    /** A map from (shape_id, shape_pt_sequence) to shapes */
    private Map<Object[], Shape> wrapped;

    public ShapeMap (ConcurrentNavigableMap<Object[], Shape> allShapes, String shapeId) {
        this.wrapped = allShapes.subMap(
            new Object[]{shapeId, 0},
            new Object[]{shapeId, null}
        );
        this.shapeId = shapeId;
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return wrapped.containsKey(makeKey(key));
    }

    @Override
    public boolean containsValue(Object value) {
        return wrapped.containsValue(value);
    }

    @Override
    public Shape get(Object key) {		
        return wrapped.get(makeKey(key));
    }

    @Override
    public Shape put(Integer key, Shape value) {
        return wrapped.put(makeKey(key), value);
    }

    @Override
    public Shape remove(Object key) {
        return wrapped.remove(makeKey(key));
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends Shape> m) {
        for (Integer i : m.keySet()) {
            wrapped.put(makeKey(i), m.get(i));
        }
    }

    @Override
    public void clear() {
        wrapped.clear();
    }

    @Override
    public Collection<Shape> values() {
        return wrapped.values();
    }

    // these two are hard because the sets have to update the corresponding map.
    // We currently just expose them as immutable sets in RAM, since all of the modification operations are optional. 
    @Override
    public Set<Integer> keySet() {
        // use a linkedhashset so values come out in order
        Set<Integer> ret = new LinkedHashSet<>();

        for (Object[] t : wrapped.keySet()) {
            ret.add((Integer) t[1]);
        }

        // Don't let the user modify the set as it won't do what they expect (change the map)
        return Collections.unmodifiableSet(ret);
    }

    @Override
    public Set<Map.Entry<Integer, Shape>> entrySet() {
        // it's ok to pull all the values into RAM as this represents a single shape not all shapes
        // use a linkedhashset so values come out in order
        Set<Entry<Integer, Shape>> ret = new LinkedHashSet<>();

        for (Map.Entry<Object[], Shape> e : wrapped.entrySet()) {
            ret.add(new AbstractMap.SimpleImmutableEntry(e.getKey()[1], e.getValue()));
        }

        return Collections.unmodifiableSet(ret);
    }

    private Object[] makeKey (Object i) {
        return new Object[]{this.shapeId, (Integer) i};
    }

}
