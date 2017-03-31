package com.conveyal.gtfs.storage;

import com.conveyal.gtfs.model.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by abyrd on 2017-03-27
 */
public class ObjectRelationalMapping<T extends Entity> {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectRelationalMapping.class);

    public static final int INSERT_BATCH_SIZE = 200;

    public final Class<T> entityClass;

    public String tableName;

    private PreparedStatement insertStatement;

    private PreparedStatement selectStatement;

    private List<Field> fields;

    private Constructor<T> noArgConstructor;

    private Connection connection;

    int batchSize;

    // TODO tableName should be provided by a method on the entity class
    public ObjectRelationalMapping (Class<T> entityClass, String tableName, Connection connection) {
        this.entityClass = entityClass;
        this.tableName = tableName;
        this.connection = connection;
        // Get all public fields going up the class hierarchy. TODO filter with annotations.
        fields = Arrays.stream(this.entityClass.getFields())
            .filter(ObjectRelationalMapping::shouldBeMapped).collect(Collectors.toList());
        try {
            noArgConstructor = this.entityClass.getConstructor();
            // H2 can't prepare a statement for a table that doesn't exist yet.
            // for now just create and wipe the table here.
            // Ideally associating a mapping to a connection and preparing the statements should be separate from creating the mapping.
            createTable();
            insertStatement = connection.prepareStatement(generateInsertSql());
            selectStatement = connection.prepareStatement(generateSelectSql());
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public void createTable () {
        String fieldDeclarations = fields.stream()
            .map(ObjectRelationalMapping::getSqlDeclaration).collect(Collectors.joining(", "));
        String sql = String.format("create table %s (%s)", tableName, fieldDeclarations);
        try {
            Statement statement = connection.createStatement();
            statement.execute("drop table if exists " + tableName);
            LOG.info(sql);
            statement.execute(sql);
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public void store (T entity) {
        try {
            int fieldIndex = 1; // Prepared statements use 1-based indexes
            for (Field field : fields) {
                Class<?> type = field.getType();
                if (type == int.class) {
                    insertStatement.setInt(fieldIndex, field.getInt(entity));
                } else if (type == long.class) {
                    insertStatement.setLong(fieldIndex, field.getLong(entity));
                } else if (type == double.class) {
                    insertStatement.setDouble(fieldIndex, field.getDouble(entity));
                } else if (type == String.class) {
                    insertStatement.setString(fieldIndex, (String) field.get(entity));
                } else if (type == URL.class) {
                    URL url = (URL) field.get(entity);
                    insertStatement.setString(fieldIndex, url == null ? null : url.toString());
                } else {
                    throw new StorageException("Attempted to store unrecognized field type: " + type);
                }
                fieldIndex += 1;
            }
            insertStatement.addBatch();
            batchSize += 1;
            if (batchSize > 1000) {
                insertStatement.executeBatch();
                batchSize = 0;
            }
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public T retrieve (String key) {
        try {
            T entity = noArgConstructor.newInstance();
            // ResultSet resultSet = selectStatement.execute();
            int fieldIndex = 0;
            for (Field field : fields) {
                Class<?> type = field.getType();
                field.set(entity, null);
                fieldIndex += 1;
            }
            return entity;
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public String generateInsertSql () {
        String questionMarks = String.join(",", Collections.nCopies(fields.size(), "?"));
        String sql = String.format("insert into %s (%s) values (%s)", tableName, getJoinedFieldNames(), questionMarks);
        return sql;
    }

    public String generateSelectSql () {
        String sql = String.format("select (%s) from %s", getJoinedFieldNames(), tableName);
        return sql;
    }

    private static boolean shouldBeMapped (Field field) {
        return !(Modifier.isStatic(field.getModifiers()));
    }

    private static String getSqlDeclaration (Field field) {
        return String.join(" ", field.getName(), getSqlType(field.getType()));
    }

    private static String getSqlType (Class<?> clazz) {
        if (clazz == int.class) return "integer";
        if (clazz == long.class) return "bigint";
        if (clazz == double.class) return "double precision";
        if (clazz == String.class) return "varchar";
        if (clazz == java.net.URL.class) return "varchar";
        throw new RuntimeException("Unrecognized type in ORM: " + clazz.getName());
    }

    private String getJoinedFieldNames () {
        return fields.stream().map(Field::getName).collect(Collectors.joining(", "));
    }

    //    public isSanitized (String sql) {
//
//    }

}
