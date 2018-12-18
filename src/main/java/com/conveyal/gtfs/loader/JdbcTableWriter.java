package com.conveyal.gtfs.loader;

import com.conveyal.gtfs.model.Entity;
import com.conveyal.gtfs.model.PatternStop;
import com.conveyal.gtfs.model.StopTime;
import com.conveyal.gtfs.storage.StorageException;
import com.conveyal.gtfs.util.InvalidNamespaceException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.conveyal.gtfs.loader.JdbcGtfsLoader.INSERT_BATCH_SIZE;
import static com.conveyal.gtfs.util.Util.ensureValidNamespace;

/**
 * This wraps a single database table and provides methods to modify GTFS entities.
 */
public class JdbcTableWriter implements TableWriter {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcTableWriter.class);
    private final DataSource dataSource;
    private final Table specTable;
    private final String tablePrefix;
    private static final ObjectMapper mapper = new ObjectMapper();
    private final Connection connection;
    private static final String RECONCILE_STOPS_ERROR_MSG = "Changes to trip pattern stops must be made one at a time if pattern contains at least one trip.";

    public JdbcTableWriter(Table table, DataSource datasource, String namespace) throws InvalidNamespaceException {
        this(table, datasource, namespace, null);
    }

    /**
     * Enum containing available methods for updating in SQL.
     */
    private enum SqlMethod {
        DELETE, UPDATE, CREATE
    }

    public JdbcTableWriter (
        Table specTable,
        DataSource dataSource,
        String tablePrefix,
        Connection optionalConnection
    ) throws InvalidNamespaceException {
        // verify tablePrefix (namespace) is ok to use for constructing dynamic sql statements
        ensureValidNamespace(tablePrefix);

        this.tablePrefix = tablePrefix;
        this.dataSource = dataSource;

        // TODO: verify specTable.name is ok to use for constructing dynamic sql statements
        this.specTable = specTable;
        // Below is a bit messy because the connection field on this class is set to final and we cannot set this to
        // the optionally passed-in connection with the ternary statement unless connection1 already exists.
        Connection connection1;
        try {
            connection1 = this.dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            connection1 = null;
        }
        if (optionalConnection != null) {
            DbUtils.closeQuietly(connection1);
        }
        this.connection = optionalConnection == null ? connection1 : optionalConnection;
    }

    /**
     * Wrapper method to call Jackson to deserialize a JSON string into JsonNode.
     */
    private static JsonNode getJsonNode (String json) throws IOException {
        try {
            return mapper.readTree(json);
        } catch (IOException e) {
            LOG.error("Bad JSON syntax", e);
            throw e;
        }
    }

    /**
     * Create a new entity in the database from the provided JSON string. Note, any call to create or update must provide
     * a JSON string with the full set of fields matching the definition of the GTFS table in the Table class.
     */
    @Override
    public String create(String json, boolean autoCommit) throws SQLException, IOException {
        return update(null, json, autoCommit);
    }

    /**
     * Update entity for a given ID with the provided JSON string. This update and any potential cascading updates to
     * referencing tables all happens in a single transaction. Note, any call to create or update must provide
     * a JSON string with the full set of fields matching the definition of the GTFS table in the Table class.
     */
    @Override
    public String update(Integer id, String json, boolean autoCommit) throws SQLException, IOException {
        final boolean isCreating = id == null;
        JsonNode jsonNode = getJsonNode(json);
        try {
            if (jsonNode.isArray()) {
                // If an array of objects is passed in as the JSON input, update them all in a single transaction, only
                // committing once all entities have been updated.
                List<String> updatedObjects = new ArrayList<>();
                for (JsonNode node : jsonNode) {
                    JsonNode idNode = node.get("id");
                    Integer nodeId = idNode == null || isCreating ? null : idNode.asInt();
                    String updatedObject = update(nodeId, node.toString(), false);
                    updatedObjects.add(updatedObject);
                }
                if (autoCommit) connection.commit();
                return mapper.writeValueAsString(updatedObjects);
            }
            // Cast JsonNode to ObjectNode to allow mutations (e.g., updating the ID field).
            ObjectNode jsonObject = (ObjectNode) jsonNode;
            // Ensure that the key field is unique and that referencing tables are updated if the value is updated.
            ensureReferentialIntegrity(connection, jsonObject, tablePrefix, specTable, id);
            // Parse the fields/values into a Field -> String map (drops ALL fields not explicitly listed in spec table's
            // fields)
            // Note, this must follow referential integrity check because some tables will modify the jsonObject (e.g.,
            // adding trip ID if it is null).
//            LOG.info("JSON to {} entity: {}", isCreating ? "create" : "update", jsonObject.toString());
            PreparedStatement preparedStatement = createPreparedUpdate(id, isCreating, jsonObject, specTable, connection, false);
            // ID from create/update result
            long newId = handleStatementExecution(preparedStatement, isCreating);
            // At this point, the transaction was successful (but not yet committed). Now we should handle any update
            // logic that applies to child tables. For example, after saving a trip, we need to store its stop times.
            Set<Table> referencingTables = getReferencingTables(specTable);
            // FIXME: hacky hack hack to add shapes table if we're updating a pattern.
            if (specTable.name.equals("patterns")) {
                referencingTables.add(Table.SHAPES);
            }
            // Iterate over referencing (child) tables and update those rows that reference the parent entity with the
            // JSON array for the key that matches the child table's name (e.g., trip.stop_times array will trigger
            // update of stop_times with matching trip_id).
            for (Table referencingTable : referencingTables) {
                Table parentTable = referencingTable.getParentTable();
                if (parentTable != null && parentTable.name.equals(specTable.name) || referencingTable.name.equals("shapes")) {
                    // If a referencing table has the current table as its parent, update child elements.
                    JsonNode childEntities = jsonObject.get(referencingTable.name);
                    if (childEntities == null || childEntities.isNull() || !childEntities.isArray()) {
                        throw new SQLException(String.format("Child entities %s must be an array and not null", referencingTable.name));
                    }
                    int entityId = isCreating ? (int) newId : id;
                    // Cast child entities to array node to iterate over.
                    ArrayNode childEntitiesArray = (ArrayNode)childEntities;
                    updateChildTable(childEntitiesArray, entityId, isCreating, referencingTable, connection);
                }
            }
            // Iterate over table's fields and apply linked values to any tables. This is to account for "exemplar"
            // fields that exist in one place in our tables, but are duplicated in GTFS. For example, we have a
            // Route#wheelchair_accessible field, which is used to set the Trip#wheelchair_accessible values for all
            // trips on a route.
            // NOTE: pattern_stops linked fields are updated in the updateChildTable method.
            switch (specTable.name) {
                case "routes":
                    updateLinkedFields(
                        specTable,
                        jsonObject,
                        "trips",
                        "route_id",
                        "wheelchair_accessible"
                    );
                    break;
                case "patterns":
                    updateLinkedFields(
                        specTable,
                        jsonObject,
                        "trips",
                        "pattern_id",
                        "direction_id", "shape_id"
                    );
                    break;
                default:
                    LOG.debug("No linked fields to update.");
                    // Do nothing.
                    break;
            }
            if (autoCommit) {
                // If nothing failed up to this point, it is safe to assume there were no problems updating/creating the
                // main entity and any of its children, so we commit the transaction.
                LOG.info("Committing transaction.");
                connection.commit();
            }
            // Add new ID to JSON object.
            jsonObject.put("id", newId);
            // FIXME: Should this return the entity freshly queried from the database rather than just updating the ID?
            return jsonObject.toString();
        } catch (Exception e) {
            LOG.error("Error {} {} entity", isCreating ? "creating" : "updating", specTable.name);
            e.printStackTrace();
            throw e;
        } finally {
            if (autoCommit) {
                // Always rollback and close in finally in case of early returns or exceptions.
                connection.rollback();
                connection.close();
            }
        }
    }

    /**
     * Updates linked fields with values from entity being updated. This is used to update identical fields in related
     * tables (for now just fields in trips and stop_times) where the reference table's value should take precedence over
     * the related table (e.g., pattern_stop#timepoint should update all of its related stop_times).
     */
    private void updateLinkedFields(Table referenceTable, ObjectNode jsonObject, String tableName, String keyField, String ...fieldNames) throws SQLException {
        boolean updatingStopTimes = "stop_times".equals(tableName);
        // Collect fields, the JSON values for these fields, and the strings to add to the prepared statement into Lists.
        List<Field> fields = new ArrayList<>();
        List<JsonNode> values = new ArrayList<>();
        List<String> fieldStrings = new ArrayList<>();
        for (String field : fieldNames) {
            fields.add(referenceTable.getFieldForName(field));
            values.add(jsonObject.get(field));
            fieldStrings.add(String.format("%s = ?", field));
        }
        String setFields = String.join(", ", fieldStrings);
        // If updating stop_times, use a more complex query that joins trips to stop_times in order to match on pattern_id
        Field orderField = updatingStopTimes ? referenceTable.getFieldForName(referenceTable.getOrderFieldName()) : null;
        String sql = updatingStopTimes
            ? String.format(
                "update %s.stop_times st set %s from %s.trips t " +
                    "where st.trip_id = t.trip_id AND t.%s = ? AND st.%s = ?",
                tablePrefix,
                setFields,
                tablePrefix,
                keyField,
                orderField.name
            )
            : String.format("update %s.%s set %s where %s = ?", tablePrefix, tableName, setFields, keyField);
        // Prepare the statement and set statement parameters
        PreparedStatement statement = connection.prepareStatement(sql);
        int oneBasedIndex = 1;
        // Iterate over list of fields that need to be updated and set params.
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            String newValue = values.get(i).isNull() ? null : values.get(i).asText();
            if (newValue == null) field.setNull(statement, oneBasedIndex++);
            else field.setParameter(statement, oneBasedIndex++, newValue);
        }
        // Set "where clause" with value for key field (e.g., set values where pattern_id = '3')
        statement.setString(oneBasedIndex++, jsonObject.get(keyField).asText());
        if (updatingStopTimes) {
            // If updating stop times set the order field parameter (stop_sequence)
            String orderValue = jsonObject.get(orderField.name).asText();
            orderField.setParameter(statement, oneBasedIndex++, orderValue);
        }
        // Log query, execute statement, and log result.
        LOG.debug(statement.toString());
        int entitiesUpdated = statement.executeUpdate();
        LOG.debug("{} {} linked fields updated", entitiesUpdated, tableName);
    }

    /**
     * Creates a prepared statement for an entity create or update operation. If not performing a batch operation, the
     * method will set parameters for the prepared statement with values found in the provided JSON ObjectNode. The Table
     * object here is provided as a positional argument (rather than provided via the JdbcTableWriter instance field)
     * because this method is used to update both the specTable for the primary entity and any relevant child entities.
     */
    private PreparedStatement createPreparedUpdate(Integer id, boolean isCreating, ObjectNode jsonObject, Table table, Connection connection, boolean batch) throws SQLException {
        String statementString;
        if (isCreating) {
            statementString = table.generateInsertSql(tablePrefix, true);
        } else {
            statementString = table.generateUpdateSql(tablePrefix, id);
        }
        // Set the RETURN_GENERATED_KEYS flag on the PreparedStatement because it may be creating new rows, in which
        // case we need to know the auto-generated IDs of those new rows.
        PreparedStatement preparedStatement = connection.prepareStatement(
                statementString,
                Statement.RETURN_GENERATED_KEYS);
        if (!batch) {
            setStatementParameters(jsonObject, table, preparedStatement, connection);
        }
        return preparedStatement;
    }

    /**
     * Given a prepared statement (for update or create), set the parameters of the statement based on string values
     * taken from JSON. Note, string values are used here in order to take advantage of setParameter method on
     * individual fields, which handles parsing string and non-string values into the appropriate SQL field types.
     */
    private void setStatementParameters(ObjectNode jsonObject, Table table, PreparedStatement preparedStatement, Connection connection) throws SQLException {
        // JDBC SQL statements use a one-based index for setting fields/parameters
        List<String> missingFieldNames = new ArrayList<>();
        int index = 1;
        for (Field field : table.editorFields()) {
            if (!jsonObject.has(field.name)) {
                // If there is a field missing from the JSON string and it is required to write to an editor table,
                // throw an exception (handled after the fields iteration. In an effort to keep the database integrity
                // intact, every update/create operation should have all fields defined by the spec table.
                // FIXME: What if someone wants to make updates to non-editor feeds? In this case, the table may not
                // have all of the required fields, yet this would prohibit such an update. Further, an update on such
                // a table that DID have all of the spec table fields would fail because they might be missing from
                // the actual database table.
                missingFieldNames.add(field.name);
                continue;
            }
            JsonNode value = jsonObject.get(field.name);
            LOG.debug("{}={}", field.name, value);
            try {
                if (value == null || value.isNull()) {
                    if (field.isRequired() && !field.isEmptyValuePermitted()) {
                        // Only register the field as missing if the value is null, the field is required, and empty
                        // values are not permitted. For example, a null value for fare_attributes#transfers should not
                        // trigger a missing field exception.
                        missingFieldNames.add(field.name);
                        continue;
                    }
                    // Handle setting null value on statement
                    field.setNull(preparedStatement, index);
                } else {
                    List<String> values = new ArrayList<>();
                    if (value.isArray()) {
                        for (JsonNode node : value) {
                            values.add(node.asText());
                        }
                        field.setParameter(preparedStatement, index, String.join(",", values));
                    } else {
                        field.setParameter(preparedStatement, index, value.asText());
                    }
                }
            } catch (StorageException e) {
                LOG.warn("Could not set field {} to value {}. Attempting to parse integer seconds.", field.name, value);
                if (field.name.contains("_time")) {
                    // FIXME: This is a hack to get arrival and departure time into the right format. Because the UI
                    // currently returns them as seconds since midnight rather than the Field-defined format HH:MM:SS.
                    try {
                        if (value == null || value.isNull()) {
                            if (field.isRequired()) {
                                missingFieldNames.add(field.name);
                                continue;
                            }
                            field.setNull(preparedStatement, index);
                        } else {
                            // Try to parse integer seconds value
                            preparedStatement.setInt(index, Integer.parseInt(value.asText()));
                            LOG.info("Parsing value {} for field {} successful!", value, field.name);
                        }
                    } catch (NumberFormatException ex) {
                        // Attempt to set arrival or departure time via integer seconds failed. Rollback.
                        connection.rollback();
                        LOG.error("Bad column: {}={}", field.name, value);
                        ex.printStackTrace();
                        throw ex;
                    }
                } else {
                    // Rollback transaction and throw exception
                    connection.rollback();
                    throw e;
                }
            }
            index += 1;
        }
        if (missingFieldNames.size() > 0) {
//            String joinedFieldNames = missingFieldNames.stream().collect(Collectors.joining(", "));
            throw new SQLException(String.format("The following field(s) are missing from JSON %s object: %s", table.name, missingFieldNames.toString()));
        }
    }

    /**
     * This updates those tables that depend on the table currently being updated. For example, if updating/creating a
     * pattern, this method handles deleting any pattern stops and shape points. For trips, this would handle updating
     * the trips' stop times.
     *
     * This method should only be used on tables that have a single foreign key reference to another table, i.e., they
     * have a hierarchical relationship.
     * FIXME develop a better way to update tables with foreign keys to the table being updated.
     */
    private void updateChildTable(ArrayNode subEntities, Integer id, boolean isCreatingNewEntity, Table subTable, Connection connection) throws SQLException, IOException {
        // Get parent table's key field
        Field keyField;
        String keyValue;
        // Primary key fields are always referenced by foreign key fields with the same name.
        keyField = specTable.getFieldForName(subTable.getKeyFieldName());
        // Get parent entity's key value
        keyValue = getValueForId(id, keyField.name, tablePrefix, specTable, connection);
        String childTableName = String.join(".", tablePrefix, subTable.name);
        // Reconciling pattern stops MUST happen before original pattern stops are deleted in below block (with
        // getUpdateReferencesSql)
        if ("pattern_stops".equals(subTable.name)) {
            List<PatternStop> newPatternStops = new ArrayList<>();
            // Clean up pattern stop ID fields (passed in as string ID from datatools-ui to avoid id collision)
            for (JsonNode node : subEntities) {
                ObjectNode objectNode = (ObjectNode) node;
                if (!objectNode.get("id").isNumber()) {
                    // Set ID to zero. ID is ignored entirely here. When the pattern stops are stored in the database,
                    // the ID values are determined by auto-incrementation.
                    objectNode.put("id", 0);
                }
                // Accumulate new pattern stop objects from JSON.
                newPatternStops.add(mapper.readValue(objectNode.toString(), PatternStop.class));
            }
            reconcilePatternStops(keyValue, newPatternStops, connection);
        }
        if (!isCreatingNewEntity) {
            // Delete existing sub-entities for given entity ID if the parent entity is not being newly created.
            String deleteSql = getUpdateReferencesSql(SqlMethod.DELETE, childTableName, keyField, keyValue, null);
            LOG.info(deleteSql);
            Statement statement = connection.createStatement();
            // FIXME: Use copy on update for a pattern's shape instead of deleting the previous shape and replacing it.
            //   This would better account for GTFS data loaded from a file where multiple patterns reference a single
            //   shape.
            int result = statement.executeUpdate(deleteSql);
            LOG.info("Deleted {} {}", result, subTable.name);
            // FIXME: are there cases when an update should not return zero?
//            if (result == 0) throw new SQLException("No stop times found for trip ID");
        }
        int entityCount = 0;
        PreparedStatement insertStatement = null;
        // Iterate over the entities found in the array and add to batch for inserting into table.
        String orderFieldName = subTable.getOrderFieldName();
        boolean hasOrderField = orderFieldName != null;
        int previousOrder = -1;
        TIntSet orderValues = new TIntHashSet();
        Multimap<Table, String> referencesPerTable = HashMultimap.create();
        for (JsonNode entityNode : subEntities) {
            // Cast entity node to ObjectNode to allow mutations (JsonNode is immutable).
            ObjectNode subEntity = (ObjectNode)entityNode;
            // Always override the key field (shape_id for shapes, pattern_id for patterns) regardless of the entity's
            // actual value.
            subEntity.put(keyField.name, keyValue);
            // Check any references the sub entity might have. For example, this checks that stop_id values on
            // pattern_stops refer to entities that actually exist in the stops table. NOTE: This skips the "specTable",
            // i.e., for pattern stops it will not check pattern_id references. This is enforced above with the put key
            // field statement above.
            for (Field field : subTable.specFields()) {
                if (field.referenceTable != null && !field.referenceTable.name.equals(specTable.name)) {
                    JsonNode refValueNode = subEntity.get(field.name);
                    // Skip over references that are null but not required (e.g., route_id in fare_rules).
                    if (refValueNode.isNull() && !field.isRequired()) continue;
                    String refValue = refValueNode.asText();
                    referencesPerTable.put(field.referenceTable, refValue);
                }
            }
            // Insert new sub-entity.
            if (entityCount == 0) {
                // If handling first iteration, create the prepared statement (later iterations will add to batch).
                insertStatement = createPreparedUpdate(id, true, subEntity, subTable, connection, true);
            }
            // Update linked stop times fields for each updated pattern stop (e.g., timepoint, pickup/drop off type).
            if ("pattern_stops".equals(subTable.name)) {
                updateLinkedFields(
                        subTable,
                        subEntity,
                        "stop_times",
                        "pattern_id",
                        "timepoint",
                        "drop_off_type",
                        "pickup_type",
                        "shape_dist_traveled"
                );
            }
            setStatementParameters(subEntity, subTable, insertStatement, connection);
            if (hasOrderField) {
                // If the table has an order field, check that it is zero-based and incrementing for all sub entities.
                // NOTE: Rather than coercing the order values to conform to the sequence in which they are found, we
                // check the values here as a sanity check.
                int orderValue = subEntity.get(orderFieldName).asInt();
                boolean orderIsUnique = orderValues.add(orderValue);
                boolean valuesAreIncrementing = ++previousOrder == orderValue;
                if (!orderIsUnique || !valuesAreIncrementing) {
                    throw new SQLException(String.format(
                            "%s %s values must be zero-based, unique, and incrementing. Entity at index %d had %s value of %d",
                            subTable.name,
                            orderFieldName,
                            entityCount,
                            previousOrder == 0 ? "non-zero" : !valuesAreIncrementing ? "non-incrementing" : "duplicate",
                            orderValue)
                    );
                }
            }
            // Log statement on first iteration so that it is not logged for each item in the batch.
            if (entityCount == 0) LOG.info(insertStatement.toString());
            insertStatement.addBatch();
            // Prefix increment count and check whether to execute batched update.
            if (++entityCount % INSERT_BATCH_SIZE == 0) {
                LOG.info("Executing batch insert ({}/{}) for {}", entityCount, subEntities.size(), childTableName);
                int[] newIds = insertStatement.executeBatch();
                LOG.info("Updated {}", newIds.length);
            }
        }
        // Check that accumulated references all exist in reference tables.
        verifyReferencesExist(subTable.name, referencesPerTable);
        // execute any remaining prepared statement calls
        LOG.info("Executing batch insert ({}/{}) for {}", entityCount, subEntities.size(), childTableName);
        if (insertStatement != null) {
            // If insert statement is null, an empty array was passed for the child table, so the child elements have
            // been wiped.
            int[] newIds = insertStatement.executeBatch();
            LOG.info("Updated {} {} child entities", newIds.length, subTable.name);
        } else {
            LOG.info("No inserts to execute. Empty array found in JSON for child table {}", childTableName);
        }
    }

    /**
     * Checks that a set of string references to a set of reference tables are all valid. For each set of references
     * mapped to a reference table, the method queries for all of the references. If there are any references that were
     * not returned in the query, one of the original references was invalid and an exception is thrown.
     * @param referringTableName    name of the table which contains references for logging/exception message only
     * @param referencesPerTable    string references mapped to the tables to which they refer
     * @throws SQLException
     */
    private void verifyReferencesExist(String referringTableName, Multimap<Table, String> referencesPerTable) throws SQLException {
        for (Table referencedTable: referencesPerTable.keySet()) {
            LOG.info("Checking {} references to {}", referringTableName, referencedTable.name);
            Collection<String> referenceStrings = referencesPerTable.get(referencedTable);
            String referenceFieldName = referencedTable.getKeyFieldName();
            String questionMarks = String.join(", ", Collections.nCopies(referenceStrings.size(), "?"));
            String checkCountSql = String.format(
                    "select %s from %s.%s where %s in (%s)",
                    referenceFieldName,
                    tablePrefix,
                    referencedTable.name,
                    referenceFieldName,
                    questionMarks);
            PreparedStatement preparedStatement = connection.prepareStatement(checkCountSql);
            int oneBasedIndex = 1;
            for (String ref : referenceStrings) {
                preparedStatement.setString(oneBasedIndex++, ref);
            }
            LOG.info(preparedStatement.toString());
            ResultSet resultSet = preparedStatement.executeQuery();
            Set<String> foundReferences = new HashSet<>();
            while (resultSet.next()) {
                String referenceValue = resultSet.getString(1);
                foundReferences.add(referenceValue);
            }
            // Determine if any references were not found.
            referenceStrings.removeAll(foundReferences);
            if (referenceStrings.size() > 0) {
                throw new SQLException(
                        String.format(
                                "%s entities must contain valid %s references. (Invalid references: %s)",
                                referringTableName,
                                referenceFieldName,
                                String.join(", ", referenceStrings)));
            } else {
                LOG.info("All {} {} {} references are valid.", foundReferences.size(), referencedTable.name, referenceFieldName);
            }
        }
    }

    /**
     * Update the trip pattern stops and the associated stop times. See extensive discussion in ticket
     * conveyal/gtfs-editor#102.
     *
     * We assume only one stop has changed---either it's been removed, added or moved. The only other case that is
     * permitted is adding a set of stops to the end of the original list. These conditions are evaluated by simply
     * checking the lengths of the original and new pattern stops (and ensuring that stop IDs remain the same where
     * required).
     *
     * If the change to pattern stops does not satisfy one of these cases, fail the update operation.
     *
     */
    private void reconcilePatternStops(String patternId, List<PatternStop> newStops, Connection connection) throws SQLException {
        LOG.info("Reconciling pattern stops for pattern ID={}", patternId);
        // Collect the original list of pattern stop IDs.
        String getStopIdsSql = String.format("select stop_id from %s.pattern_stops where pattern_id = ? order by stop_sequence",
                tablePrefix);
        PreparedStatement getStopsStatement = connection.prepareStatement(getStopIdsSql);
        getStopsStatement.setString(1, patternId);
        LOG.info(getStopsStatement.toString());
        ResultSet stopsResults = getStopsStatement.executeQuery();
        List<String> originalStopIds = new ArrayList<>();
        while (stopsResults.next()) {
            originalStopIds.add(stopsResults.getString(1));
        }

        // Collect all trip IDs so that we can insert new stop times (with the appropriate trip ID value) if a pattern
        // stop is added.
        String getTripIdsSql = String.format("select trip_id from %s.trips where pattern_id = ?", tablePrefix);
        PreparedStatement getTripsStatement = connection.prepareStatement(getTripIdsSql);
        getTripsStatement.setString(1, patternId);
        ResultSet tripsResults = getTripsStatement.executeQuery();
        List<String> tripsForPattern = new ArrayList<>();
        while (tripsResults.next()) {
            tripsForPattern.add(tripsResults.getString(1));
        }

        if (tripsForPattern.size() == 0) {
            // If there are no trips for the pattern, there is no need to reconcile stop times to modified pattern stops.
            // This permits the creation of patterns without stops, reversing the stops on existing patterns, and
            // duplicating patterns.
            // For new patterns, this short circuit is required to prevent the transposition conditional check from
            // throwing an IndexOutOfBoundsException when it attempts to access index 0 of a list with no items.
            return;
        }
        // Prepare SQL fragment to filter for all stop times for all trips on a certain pattern.
        String joinToTrips = String.format("%s.trips.trip_id = %s.stop_times.trip_id AND %s.trips.pattern_id = '%s'",
                tablePrefix, tablePrefix, tablePrefix, patternId);

        // ADDITIONS (IF DIFF == 1)
        if (originalStopIds.size() == newStops.size() - 1) {
            // We have an addition; find it.
            int differenceLocation = -1;
            for (int i = 0; i < newStops.size(); i++) {
                if (differenceLocation != -1) {
                    // we've already found the addition
                    if (i < originalStopIds.size() && !originalStopIds.get(i).equals(newStops.get(i + 1).stop_id)) {
                        // there's another difference, which we weren't expecting
                        throw new IllegalStateException("Multiple differences found when trying to detect stop addition");
                    }
                }

                // if we've reached where one trip has an extra stop, or if the stops at this position differ
                else if (i == newStops.size() - 1 || !originalStopIds.get(i).equals(newStops.get(i).stop_id)) {
                    // we have found the difference
                    differenceLocation = i;
                }
            }
            // Increment sequences for stops that follow the inserted location (including the stop at the changed index).
            // NOTE: This should happen before the blank stop time insertion for logical consistency.
            String updateSql = String.format(
                "update %s.stop_times set stop_sequence = stop_sequence + 1 from %s.trips where stop_sequence >= %d AND %s",
                tablePrefix,
                tablePrefix,
                differenceLocation,
                joinToTrips
            );
            LOG.info(updateSql);
            PreparedStatement updateStatement = connection.prepareStatement(updateSql);
            int updated = updateStatement.executeUpdate();
            LOG.info("Updated {} stop times", updated);

            // Insert a skipped stop at the difference location
            insertBlankStopTimes(tripsForPattern, newStops, differenceLocation, 1, connection);
        }

        // DELETIONS
        else if (originalStopIds.size() == newStops.size() + 1) {
            // We have a deletion; find it
            int differenceLocation = -1;
            for (int i = 0; i < originalStopIds.size(); i++) {
                if (differenceLocation != -1) {
                    if (!originalStopIds.get(i).equals(newStops.get(i - 1).stop_id)) {
                        // There is another difference, which we were not expecting
                        throw new IllegalStateException("Multiple differences found when trying to detect stop removal");
                    }
                } else if (i == originalStopIds.size() - 1 || !originalStopIds.get(i).equals(newStops.get(i).stop_id)) {
                    // We've reached the end and the only difference is length (so the last stop is the different one)
                    // or we've found the difference.
                    differenceLocation = i;
                }
            }
            // Delete stop at difference location
            String deleteSql = String.format(
                "delete from %s.stop_times using %s.trips where stop_sequence = %d AND %s",
                tablePrefix,
                tablePrefix,
                differenceLocation,
                joinToTrips
            );
            LOG.info(deleteSql);
            PreparedStatement deleteStatement = connection.prepareStatement(deleteSql);
            // Decrement all stops with sequence greater than difference location
            String updateSql = String.format(
                "update %s.stop_times set stop_sequence = stop_sequence - 1 from %s.trips where stop_sequence > %d AND %s",
                tablePrefix,
                tablePrefix,
                differenceLocation,
                joinToTrips
            );
            LOG.info(updateSql);
            PreparedStatement updateStatement = connection.prepareStatement(updateSql);
            int deleted = deleteStatement.executeUpdate();
            int updated = updateStatement.executeUpdate();
            LOG.info("Deleted {} stop times, updated sequence for {} stop times", deleted, updated);

            // FIXME: Should we be handling bad stop time delete? I.e., we could query for stop times to be deleted and
            // if any of them have different stop IDs than the pattern stop, we could raise a warning for the user.
            String removedStopId = originalStopIds.get(differenceLocation);
//            StopTime removed = trip.stopTimes.remove(differenceLocation);
//
//            // the removed stop can be null if it was skipped. trip.stopTimes.remove will throw an exception
//            // rather than returning null if we try to do a remove out of bounds.
//            if (removed != null && !removed.stop_id.equals(removedStopId)) {
//                throw new IllegalStateException("Attempted to remove wrong stop!");
//            }
        }

        // TRANSPOSITIONS
        else if (originalStopIds.size() == newStops.size()) {
            // Imagine the trip patterns pictured below (where . is a stop, and lines indicate the same stop)
            // the original trip pattern is on top, the new below
            // . . . . . . . .
            // | |  \ \ \  | |
            // * * * * * * * *
            // also imagine that the two that are unmarked are the same
            // (the limitations of ascii art, this is prettier on my whiteboard)
            // There are three regions: the beginning and end, where stopSequences are the same, and the middle, where they are not
            // The same is true of trips where stops were moved backwards

            // find the left bound of the changed region
            int firstDifferentIndex = 0;
            while (originalStopIds.get(firstDifferentIndex).equals(newStops.get(firstDifferentIndex).stop_id)) {
                firstDifferentIndex++;

                if (firstDifferentIndex == originalStopIds.size())
                    // trip patterns do not differ at all, nothing to do
                    return;
            }

            // find the right bound of the changed region
            int lastDifferentIndex = originalStopIds.size() - 1;
            while (originalStopIds.get(lastDifferentIndex).equals(newStops.get(lastDifferentIndex).stop_id)) {
                lastDifferentIndex--;
            }

            // TODO: write a unit test for this
            if (firstDifferentIndex == lastDifferentIndex) {
                throw new IllegalStateException(
                        "Pattern stop substitutions are not supported, region of difference must have length > 1.");
            }
            String conditionalUpdate;

            // figure out whether a stop was moved left or right
            // note that if the stop was only moved one position, it's impossible to tell, and also doesn't matter,
            // because the requisite operations are equivalent
            int from, to;
            // Ensure that only a single stop has been moved (i.e. verify stop IDs inside changed region remain unchanged)
            if (originalStopIds.get(firstDifferentIndex).equals(newStops.get(lastDifferentIndex).stop_id)) {
                // Stop was moved from beginning of changed region to end of changed region (-->)
                from = firstDifferentIndex;
                to = lastDifferentIndex;
                verifyInteriorStopsAreUnchanged(originalStopIds, newStops, firstDifferentIndex, lastDifferentIndex, true);
                conditionalUpdate = String.format("update %s.stop_times set stop_sequence = case " +
                        // if sequence = fromIndex, update to toIndex.
                        "when stop_sequence = %d then %d " +
                        // if sequence is greater than fromIndex and less than or equal to toIndex, decrement
                        "when stop_sequence > %d AND stop_sequence <= %d then stop_sequence - 1 " +
                        // Otherwise, sequence remains untouched
                        "else stop_sequence " +
                        "end " +
                        "from %s.trips where %s",
                        tablePrefix, from, to, from, to, tablePrefix, joinToTrips);
            } else if (newStops.get(firstDifferentIndex).stop_id.equals(originalStopIds.get(lastDifferentIndex))) {
                // Stop was moved from end of changed region to beginning of changed region (<--)
                from = lastDifferentIndex;
                to = firstDifferentIndex;
                verifyInteriorStopsAreUnchanged(originalStopIds, newStops, firstDifferentIndex, lastDifferentIndex, false);
                conditionalUpdate = String.format("update %s.stop_times set stop_sequence = case " +
                        // if sequence = fromIndex, update to toIndex.
                        "when stop_sequence = %d then %d " +
                        // if sequence is less than fromIndex and greater than or equal to toIndex, increment
                        "when stop_sequence < %d AND stop_sequence >= %d then stop_sequence + 1 " +
                        // Otherwise, sequence remains untouched
                        "else stop_sequence " +
                        "end " +
                        "from %s.trips where %s",
                        tablePrefix, from, to, from, to, tablePrefix, joinToTrips);
            } else {
                throw new IllegalStateException("not a simple, single move!");
            }

            // Update the stop sequences for the stop that was moved and the other stops within the changed region.
            PreparedStatement updateStatement = connection.prepareStatement(conditionalUpdate);
            LOG.info(updateStatement.toString());
            int updated = updateStatement.executeUpdate();
            LOG.info("Updated {} stop_times.", updated);
        }
        // CHECK IF SET OF STOPS ADDED TO END OF ORIGINAL LIST
        else if (originalStopIds.size() < newStops.size()) {
            // find the left bound of the changed region to check that no stops have changed in between
            int firstDifferentIndex = 0;
            while (
                    firstDifferentIndex < originalStopIds.size() &&
                    originalStopIds.get(firstDifferentIndex).equals(newStops.get(firstDifferentIndex).stop_id)
                ) {
                firstDifferentIndex++;
            }
            if (firstDifferentIndex != originalStopIds.size())
                throw new IllegalStateException("When adding multiple stops to patterns, new stops must all be at the end");

            // insert a skipped stop for each new element in newStops
            int stopsToInsert = newStops.size() - firstDifferentIndex;
            // FIXME: Should we be inserting blank stop times at all?  Shouldn't these just inherit the arrival times
            // from the pattern stops?
            LOG.info("Adding {} stop times to existing {} stop times. Starting at {}", stopsToInsert, originalStopIds.size(), firstDifferentIndex);
            insertBlankStopTimes(tripsForPattern, newStops, firstDifferentIndex, stopsToInsert, connection);
        }
        // ANY OTHER TYPE OF MODIFICATION IS NOT SUPPORTED
        else throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
    }

    /**
     * Check the stops in the changed region to ensure they remain in the same order. If not, throw an exception to
     * cancel the transaction.
     */
    private static void verifyInteriorStopsAreUnchanged(List<String> originalStopIds, List<PatternStop> newStops, int firstDifferentIndex, int lastDifferentIndex, boolean movedRight) {
        //Stops mapped to list of stop IDs simply for easier viewing/comparison with original IDs while debugging with
        // breakpoints.
        List<String> newStopIds = newStops.stream().map(s -> s.stop_id).collect(Collectors.toList());
        // Determine the bounds of the region that should be identical between the two lists.
        int beginRegion = movedRight ? firstDifferentIndex : firstDifferentIndex + 1;
        int endRegion = movedRight ? lastDifferentIndex - 1 : lastDifferentIndex;
        for (int i = beginRegion; i <= endRegion; i++) {
            // Shift index when selecting stop from original list to account for displaced stop.
            int shiftedIndex = movedRight ? i + 1 : i - 1;
            String newStopId = newStopIds.get(i);
            String originalStopId = originalStopIds.get(shiftedIndex);
            if (!newStopId.equals(originalStopId)) {
                // If stop ID for new stop at the given index does not match the original stop ID, the order of at least
                // one stop within the changed region has been changed, which is illegal according to the rule enforcing
                // only a single addition, deletion, or transposition per update.
                throw new IllegalStateException(RECONCILE_STOPS_ERROR_MSG);
            }
        }
    }

    /**
     * You must call this method after updating sequences for any stop times following the starting stop sequence to
     * avoid overwriting these other stop times.
     */
    private void insertBlankStopTimes(List<String> tripIds, List<PatternStop> newStops, int startingStopSequence, int stopTimesToAdd, Connection connection) throws SQLException {
        if (tripIds.isEmpty()) {
            // There is no need to insert blank stop times if there are no trips for the pattern.
            return;
        }
        String insertSql = Table.STOP_TIMES.generateInsertSql(tablePrefix, true);
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        int count = 0;
        int totalRowsUpdated = 0;
        // Create a new stop time for each sequence value (times each trip ID) that needs to be inserted.
        for (int i = startingStopSequence; i < stopTimesToAdd + startingStopSequence; i++) {
            PatternStop patternStop = newStops.get(i);
            StopTime stopTime = new StopTime();
            stopTime.stop_id = patternStop.stop_id;
            stopTime.drop_off_type = patternStop.drop_off_type;
            stopTime.pickup_type = patternStop.pickup_type;
            stopTime.timepoint = patternStop.timepoint;
            stopTime.shape_dist_traveled = patternStop.shape_dist_traveled;
            stopTime.stop_sequence = i;
            // Update stop time with each trip ID and add to batch.
            for (String tripId : tripIds) {
                stopTime.trip_id = tripId;
                stopTime.setStatementParameters(insertStatement, true);
                insertStatement.addBatch();
                if (count % INSERT_BATCH_SIZE == 0) {
                    int[] rowsUpdated = insertStatement.executeBatch();
                    totalRowsUpdated += rowsUpdated.length;
                }
            }
        }
        int[] rowsUpdated = insertStatement.executeBatch();
        totalRowsUpdated += rowsUpdated.length;
        LOG.info("{} blank stop times inserted", totalRowsUpdated);
    }

    /**
     * For a given condition (fieldName = 'value'), delete all entities that match the condition. Because this uses the
     * primary delete method, it also will delete any "child" entities that reference any entities matching the original
     * query.
     */
    @Override
    public int deleteWhere(String fieldName, String value, boolean autoCommit) throws SQLException {
        try {
            String tableName = String.join(".", tablePrefix, specTable.name);
            // Get the IDs for entities matching the where condition
            TIntSet idsToDelete = getIdsForCondition(tableName, fieldName, value, connection);
            TIntIterator iterator = idsToDelete.iterator();
            TIntList results = new TIntArrayList();
            while (iterator.hasNext()) {
                // For all entity IDs that match query, delete from referencing tables.
                int id = iterator.next();
                // FIXME: Should this be a where in clause instead of iterating over IDs?
                // Delete each entity and its referencing (child) entities
                int result = delete(id, false);
                if (result != 1) {
                    throw new SQLException("Could not delete entity with ID " + id);
                }
                results.add(result);
            }
            if (autoCommit) connection.commit();
            LOG.info("Deleted {} {} entities", results.size(), specTable.name);
            return results.size();
        } catch (Exception e) {
            connection.rollback();
            LOG.error("Could not delete {} entity where {}={}", specTable.name, fieldName, value);
            e.printStackTrace();
            throw e;
        } finally {
            if (autoCommit) {
                // Always rollback and close if auto-committing.
                connection.rollback();
                connection.close();
            }
        }
    }

    /**
     * Deletes an entity for the specified ID.
     */
    @Override
    public int delete(Integer id, boolean autoCommit) throws SQLException {
        try {
            // Handle "cascading" delete or constraints on deleting entities that other entities depend on
            // (e.g., keep a calendar from being deleted if trips reference it).
            // FIXME: actually add "cascading"? Currently, it just deletes one level down.
            deleteFromReferencingTables(tablePrefix, specTable, connection, id);
            PreparedStatement statement = connection.prepareStatement(specTable.generateDeleteSql(tablePrefix));
            statement.setInt(1, id);
            LOG.info(statement.toString());
            // Execute query
            int result = statement.executeUpdate();
            if (result == 0) {
                LOG.error("Could not delete {} entity with id: {}", specTable.name, id);
                throw new SQLException("Could not delete entity");
            }
            if (autoCommit) connection.commit();
            // FIXME: change return message based on result value
            return result;
        } catch (Exception e) {
            LOG.error("Could not delete {} entity with id: {}", specTable.name, id);
            e.printStackTrace();
            throw e;
        } finally {
            if (autoCommit) {
                // Always rollback and close if auto-committing.
                connection.rollback();
                connection.close();
            }
        }
    }

    @Override
    public void commit() throws SQLException {
        // FIXME: should this take a connection and commit it?
        connection.commit();
        connection.close();
    }

    /**
     * Delete entities from any referencing tables (if required). This method is defined for convenience and clarity, but
     * essentially just runs updateReferencingTables with a null value for newKeyValue param.
     */
    private static void deleteFromReferencingTables(String namespace, Table table, Connection connection, int id) throws SQLException {
        updateReferencingTables(namespace, table, connection, id, null);
    }

    /**
     * Handle executing a prepared statement and return the ID for the newly-generated or updated entity.
     */
    private static long handleStatementExecution(PreparedStatement statement, boolean isCreating) throws SQLException {
        // Log the SQL for the prepared statement
        LOG.info(statement.toString());
        int affectedRows = statement.executeUpdate();
        // Determine operation-specific action for any error messages
        String messageAction = isCreating ? "Creating" : "Updating";
        if (affectedRows == 0) {
            // No update occurred.
            // TODO: add some clarity on cause (e.g., where clause found no entity with provided ID)?
            throw new SQLException(messageAction + " entity failed, no rows affected.");
        }
        try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
            if (generatedKeys.next()) {
                // Get the auto-generated ID from the update execution
                long newId = generatedKeys.getLong(1);
                return newId;
            } else {
                throw new SQLException(messageAction + " entity failed, no ID obtained.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Checks for modification of GTFS key field (e.g., stop_id, route_id) in supplied JSON object and ensures
     * both uniqueness and that referencing tables are appropriately updated.
     *
     * FIXME: add more detail/precise language on what this method actually does
     */
    private static void ensureReferentialIntegrity(Connection connection, ObjectNode jsonObject, String namespace, Table table, Integer id) throws SQLException {
        final boolean isCreating = id == null;
        String keyField = table.getKeyFieldName();
        String tableName = String.join(".", namespace, table.name);
        if (jsonObject.get(keyField) == null || jsonObject.get(keyField).isNull()) {
            // FIXME: generate key field automatically for certain entities (e.g., trip ID). Maybe this should be
            // generated for all entities if null?
            if ("trip_id".equals(keyField)) {
                jsonObject.put(keyField, UUID.randomUUID().toString());
            } else if ("agency_id".equals(keyField)) {
                LOG.warn("agency_id field for agency id={} is null.", id);
                int rowSize = getRowCount(tableName, connection);
                if (rowSize > 1 || (isCreating && rowSize > 0)) {
                    throw new SQLException("agency_id must not be null if more than one agency exists.");
                }
            } else {
                throw new SQLException(String.format("Key field %s must not be null", keyField));
            }
        }
        String keyValue = jsonObject.get(keyField).asText();
        // If updating key field, check that there is no ID conflict on value (e.g., stop_id or route_id)
        TIntSet uniqueIds = getIdsForCondition(tableName, keyField, keyValue, connection);
        int size = uniqueIds.size();
        if (size == 0 || (size == 1 && id != null && uniqueIds.contains(id))) {
            // OK.
            if (size == 0 && !isCreating) {
                // FIXME: Need to update referencing tables because entity has changed ID.
                // Entity key value is being changed to an entirely new one.  If there are entities that
                // reference this value, we need to update them.
                updateReferencingTables(namespace, table, connection, id, keyValue);
            }
        } else {
            // Conflict. The different conflict conditions are outlined below.
            if (size == 1) {
                // There was one match found.
                if (isCreating) {
                    // Under no circumstance should a new entity have a conflict with existing key field.
                    throw new SQLException("New entity's key field must not match existing value.");
                }
                if (!uniqueIds.contains(id)) {
                    // There are two circumstances we could encounter here.
                    // 1. The key value for this entity has been updated to match some other entity's key value (conflict).
                    // 2. The int ID provided in the request parameter does not match any rows in the table.
                    throw new SQLException("Key field must be unique and request parameter ID must exist.");
                }
            } else if (size > 1) {
                // FIXME: Handle edge case where original data set contains duplicate values for key field and this is an
                // attempt to rectify bad data.
                String message = String.format(
                        "%d %s entities shares the same key field (%s=%s)! Key field must be unique.",
                        size,
                        table.name,
                        keyField,
                        keyValue);
                LOG.error(message);
                throw new SQLException(message);
            }
        }
    }

    /**
     * Get number of rows for a table. This is currently just used to check the number of entities for the agency table.
     */
    private static int getRowCount(String tableName, Connection connection) throws SQLException {
        String rowCountSql = String.format("SELECT COUNT(*) FROM %s", tableName);
        LOG.info(rowCountSql);
        // Create statement for counting rows selected
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(rowCountSql);
        if (resultSet.next()) return resultSet.getInt(1);
        else return 0;
    }

    /**
     * For some condition (where field = string value), return the set of unique int IDs for the records that match.
     */
    private static TIntSet getIdsForCondition(String tableName, String keyField, String keyValue, Connection connection) throws SQLException {
        String idCheckSql = String.format("select id from %s where %s = ?", tableName, keyField);
        // Create statement for counting rows selected
        PreparedStatement statement = connection.prepareStatement(idCheckSql);
        statement.setString(1, keyValue);
        LOG.info(statement.toString());
        ResultSet resultSet = statement.executeQuery();
        // Keep track of number of records found with key field
        TIntSet uniqueIds = new TIntHashSet();
        while (resultSet.next()) {
            int uniqueId = resultSet.getInt(1);
            uniqueIds.add(uniqueId);
            LOG.info("entity id: {}, where {}: {}", uniqueId, keyField, keyValue);
        }
        return uniqueIds;
    }

    /**
     * Finds the set of tables that reference the parent entity being updated.
     */
    private static Set<Table> getReferencingTables(Table table) {
        String keyField = table.getKeyFieldName();
        Set<Table> referencingTables = new HashSet<>();
        for (Table gtfsTable : Table.tablesInOrder) {
            // IMPORTANT: Skip the table for the entity we're modifying or if loop table does not have field.
            if (table.name.equals(gtfsTable.name)) continue;
//            || !gtfsTable.hasField(keyField)
            for (Field field : gtfsTable.fields) {
                if (field.isForeignReference() && field.referenceTable.name.equals(table.name)) {
                    // If any of the table's fields are foreign references to the specified table, add to the return set.
                    referencingTables.add(gtfsTable);
                }
            }
//            Field tableField = gtfsTable.getFieldForName(keyField);
//            // If field is not a foreign reference, continue. (This should probably never be the case because a field
//            // that shares the key field's name ought to refer to the key field.
//            if (!tableField.isForeignReference()) continue;

        }
        return referencingTables;
    }

    /**
     * For a given integer ID, return the value for the specified field name for that entity.
     */
    private static String getValueForId(int id, String fieldName, String namespace, Table table, Connection connection) throws SQLException {
        String tableName = String.join(".", namespace, table.name);
        String selectIdSql = String.format("select %s from %s where id = %d", fieldName, tableName, id);
        LOG.info(selectIdSql);
        Statement selectIdStatement = connection.createStatement();
        ResultSet selectResults = selectIdStatement.executeQuery(selectIdSql);
        String keyValue = null;
        while (selectResults.next()) {
            keyValue = selectResults.getString(1);
        }
        return keyValue;
    }

    /**
     * Updates any foreign references that exist should a GTFS key field (e.g., stop_id or route_id) be updated via an
     * HTTP request for a given integer ID. First, all GTFS tables are filtered to find referencing tables. Then records
     * in these tables that match the old key value are modified to match the new key value.
     *
     * The function determines whether the method is update or delete depending on the presence of the newKeyValue
     * parameter (if null, the method is DELETE). Custom logic/hooks could be added here to check if there are entities
     * referencing the entity being updated.
     *
     * FIXME: add custom logic/hooks. Right now entity table checks are hard-coded in (e.g., if Agency, skip all. OR if
     * Calendar, rollback transaction if there are referencing trips).
     *
     * FIXME: Do we need to clarify the impact of the direction of the relationship (e.g., if we delete a trip, that should
     * not necessarily delete a shape that is shared by multiple trips)? I think not because we are skipping foreign refs
     * found in the table for the entity being updated/deleted. [Leaving this comment in place for now though.]
     */
    private static void updateReferencingTables(String namespace, Table table, Connection connection, int id, String newKeyValue) throws SQLException {
        Field keyField = table.getFieldForName(table.getKeyFieldName());
        Class<? extends Entity> entityClass = table.getEntityClass();
        // Determine method (update vs. delete) depending on presence of newKeyValue field.
        SqlMethod sqlMethod = newKeyValue != null ? SqlMethod.UPDATE : SqlMethod.DELETE;
        Set<Table> referencingTables = getReferencingTables(table);
        // If there are no referencing tables, there is no need to update any values (e.g., .
        if (referencingTables.size() == 0) return;
        String keyValue = getValueForId(id, keyField.name, namespace, table, connection);
        if (keyValue == null) {
            // FIXME: should we still check referencing tables for null value?
            LOG.warn("Entity {} to {} has null value for {}. Skipping references check.", id, sqlMethod, keyField);
            return;
        }
        for (Table referencingTable : referencingTables) {
            // Update/delete foreign references that have match the key value.
            String refTableName = String.join(".", namespace, referencingTable.name);
            for (Field field : referencingTable.editorFields()) {
                if (field.isForeignReference() && field.referenceTable.name.equals(table.name)) {
                    // FIXME: Are there other references that are not being captured???
                    // Cascade delete stop times and frequencies for trips. This must happen before trips are deleted
                    // below. Otherwise, there are no trips with which to join.
                    if ("trips".equals(referencingTable.name)) {
                        String stopTimesTable = String.join(".", namespace, "stop_times");
                        String frequenciesTable = String.join(".", namespace, "frequencies");
                        String tripsTable = String.join(".", namespace, "trips");
                        // Delete stop times and frequencies for trips for pattern
                        String deleteStopTimes = String.format(
                                "delete from %s using %s where %s.trip_id = %s.trip_id and %s.pattern_id = '%s'",
                                stopTimesTable, tripsTable, stopTimesTable, tripsTable, tripsTable, keyValue);
                        LOG.info(deleteStopTimes);
                        PreparedStatement deleteStopTimesStatement = connection.prepareStatement(deleteStopTimes);
                        int deletedStopTimes = deleteStopTimesStatement.executeUpdate();
                        LOG.info("Deleted {} stop times for pattern {}", deletedStopTimes, keyValue);
                        String deleteFrequencies = String.format(
                                "delete from %s using %s where %s.trip_id = %s.trip_id and %s.pattern_id = '%s'",
                                frequenciesTable, tripsTable, frequenciesTable, tripsTable, tripsTable, keyValue);
                        LOG.info(deleteFrequencies);
                        PreparedStatement deleteFrequenciesStatement = connection.prepareStatement(deleteFrequencies);
                        int deletedFrequencies = deleteFrequenciesStatement.executeUpdate();
                        LOG.info("Deleted {} frequencies for pattern {}", deletedFrequencies, keyValue);
                    }
                    // Get unique IDs before delete (for logging/message purposes).
                    // TIntSet uniqueIds = getIdsForCondition(refTableName, keyField, keyValue, connection);
                    String updateRefSql = getUpdateReferencesSql(sqlMethod, refTableName, field, keyValue, newKeyValue);
                    LOG.info(updateRefSql);
                    Statement updateStatement = connection.createStatement();
                    int result = updateStatement.executeUpdate(updateRefSql);
                    if (result > 0) {
                        // FIXME: is this where a delete hook should go? (E.g., CalendarController subclass would override
                        // deleteEntityHook).
                        // deleteEntityHook();
                        if (sqlMethod.equals(SqlMethod.DELETE)) {
                            // Check for restrictions on delete.
                            if (table.isCascadeDeleteRestricted()) {
                                // The entity must not have any referencing entities in order to delete it.
                                connection.rollback();
                                //                        List<String> idStrings = new ArrayList<>();
                                //                        uniqueIds.forEach(uniqueId -> idStrings.add(String.valueOf(uniqueId)));
                                //                        String message = String.format("Cannot delete %s %s=%s. %d %s reference this %s (%s).", entityClass.getSimpleName(), keyField, keyValue, result, referencingTable.name, entityClass.getSimpleName(), String.join(",", idStrings));
                                String message = String.format("Cannot delete %s %s=%s. %d %s reference this %s.", entityClass.getSimpleName(), keyField.name, keyValue, result, referencingTable.name, entityClass.getSimpleName());
                                LOG.warn(message);
                                throw new SQLException(message);
                            }
                        }
                        LOG.info("{} reference(s) in {} {}D!", result, refTableName, sqlMethod);
                    } else {
                        LOG.info("No references in {} found!", refTableName);
                    }
                }
            }
        }
    }

    /**
     * Constructs SQL string based on method provided.
     */
    private static String getUpdateReferencesSql(SqlMethod sqlMethod, String refTableName, Field keyField, String keyValue, String newKeyValue) throws SQLException {
        boolean isArrayField = keyField.getSqlType().equals(JDBCType.ARRAY);
        switch (sqlMethod) {
            case DELETE:
                if (isArrayField) {
                    return String.format("delete from %s where %s @> ARRAY['%s']::text[]", refTableName, keyField.name, keyValue);
                } else {
                    return String.format("delete from %s where %s = '%s'", refTableName, keyField.name, keyValue);
                }
            case UPDATE:
                if (isArrayField) {
                    // If the field to be updated is an array field (of which there are only text[] types in the db),
                    // replace the old value with the new value using array contains clause.
                    // FIXME This is probably horribly postgres specific.
                    return String.format("update %s set %s = array_replace(%s, '%s', '%s') where %s @> ARRAY['%s']::text[]", refTableName, keyField.name, keyField.name, keyValue, newKeyValue, keyField.name, keyValue);
                } else {
                    return String.format("update %s set %s = '%s' where %s = '%s'", refTableName, keyField.name, newKeyValue, keyField.name, keyValue);
                }
//            case CREATE:
//                return String.format("insert into %s ");
            default:
                throw new SQLException("SQL Method must be DELETE or UPDATE.");
        }
    }
}
