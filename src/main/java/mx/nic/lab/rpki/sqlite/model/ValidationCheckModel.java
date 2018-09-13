package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.ValidationCheck;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.ValidationCheckDbObject;

/**
 * Model to retrieve Validation check data from the database
 *
 */
public class ValidationCheckModel {

	private static final Logger logger = Logger.getLogger(ValidationCheckModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "ValidationCheck";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_VALIDATION_RUN_ID = "getByValidationRunId";
	private static final String GET_BY_UNIQUE = "getByUnique";
	private static final String GET_PARAMETERS = "getParameters";
	private static final String GET_LAST_PARAMETER_ID = "getLastParameterId";
	private static final String CREATE = "create";
	private static final String CREATE_PARAMETER = "createParameter";

	/**
	 * Loads the queries corresponding to this model, based on the QUERY_GROUP
	 * constant
	 * 
	 * @param schema
	 */
	public static void loadQueryGroup(String schema) {
		try {
			QueryGroup group = new QueryGroup(QUERY_GROUP, schema);
			setQueryGroup(group);
		} catch (IOException e) {
			throw new RuntimeException("Error loading query group", e);
		}
	}

	/**
	 * Creates a new {@link ValidationCheck} returns null if the object couldn't be
	 * created.
	 * 
	 * @param newValidationCheck
	 * @param connection
	 * @return The ID of the {@link ValidationCheck} created
	 * @throws SQLException
	 */
	public static Long create(ValidationCheck newValidationCheck, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			ValidationCheckDbObject stored = new ValidationCheckDbObject(newValidationCheck);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			stored = getByUniqueFields(stored, connection);
			newValidationCheck.setId(stored.getId());
			storeRelatedObjects(newValidationCheck, connection);
			return newValidationCheck.getId();
		}
	}

	/**
	 * Get the {@link ValidationCheck}s related to a Validation Run ID
	 * 
	 * @param validationRunId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static List<ValidationCheck> getByValidationRunId(Long validationRunId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_VALIDATION_RUN_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, validationRunId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<ValidationCheck> validationChecks = new ArrayList<>();
			while (rs.next()) {
				ValidationCheckDbObject validationCheck = new ValidationCheckDbObject(rs);
				loadRelatedObjects(validationCheck, connection);
				validationChecks.add(validationCheck);
			}
			return validationChecks;
		}
	}

	/**
	 * Return a {@link PreparedStatement} that contains the necessary parameters to
	 * make a search of a unique {@link ValidationCheck} based on properties
	 * distinct that the ID
	 * 
	 * @param validationCheck
	 * @param queryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement prepareUniqueSearch(ValidationCheck validationCheck, String queryId,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(queryId);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int varIdIdx = -1;
		int locationIdx = -1;
		int keyIdx = -1;
		if (validationCheck.getValidationRun() != null) {
			parameters.append(" and ").append(ValidationCheckDbObject.VALIDATION_RUN_COLUMN).append(" = ? ");
			varIdIdx = currentIdx++;
		}
		if (validationCheck.getLocation() != null) {
			parameters.append(" and ").append(ValidationCheckDbObject.LOCATION_COLUMN).append(" = ? ");
			locationIdx = currentIdx++;
		}
		if (validationCheck.getKey() != null) {
			parameters.append(" and ").append(ValidationCheckDbObject.KEY_COLUMN).append(" = ? ");
			keyIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		PreparedStatement statement = connection.prepareStatement(query);
		if (varIdIdx > 0) {
			statement.setLong(varIdIdx, validationCheck.getValidationRun().getId());
		}
		if (locationIdx > 0) {
			statement.setString(locationIdx, validationCheck.getLocation());
		}
		if (keyIdx > 0) {
			statement.setString(keyIdx, validationCheck.getKey());
		}
		return statement;
	}

	/**
	 * Get a {@link ValidationCheck} by its unique fields
	 * 
	 * @param validationCheck
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static ValidationCheckDbObject getByUniqueFields(ValidationCheck validationCheck, Connection connection)
			throws SQLException {
		try (PreparedStatement statement = prepareUniqueSearch(validationCheck, GET_BY_UNIQUE, connection)) {
			ResultSet resultSet = statement.executeQuery();
			ValidationCheckDbObject found = new ValidationCheckDbObject(resultSet);
			loadRelatedObjects(found, connection);
			return found;
		}
	}

	/**
	 * Get the list of parameters related to a Validation Check ID
	 * 
	 * @param validationCheckId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static List<String> getParameters(Long validationCheckId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_PARAMETERS);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, validationCheckId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<String> parameters = new ArrayList<>();
			while (rs.next()) {
				parameters.add(rs.getString(ValidationCheckDbObject.PARAMETERS_COLUMN));
			}
			return parameters;
		}
	}

	/**
	 * Load the related objects to a {@link ValidationCheck}
	 * 
	 * @param validationCheck
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(ValidationCheckDbObject validationCheck, Connection connection)
			throws SQLException {
		validationCheck.setParameters(getParameters(validationCheck.getId(), connection));
	}

	/**
	 * Store the related objects to a {@link ValidationCheck}
	 * 
	 * @param validationCheck
	 * @param connection
	 * @throws SQLException
	 */
	private static void storeRelatedObjects(ValidationCheck validationCheck, Connection connection)
			throws SQLException {
		List<String> parameters = validationCheck.getParameters();
		if (parameters != null) {
			for (String parameter : parameters) {
				if (parameter != null && !parameter.trim().isEmpty()) {
					createParameter(validationCheck.getId(), parameter.trim(), connection);
				}
			}
		}
	}

	/**
	 * Creates the parameter related to a {@link ValidationCheck}, returns false if
	 * the parameter couldn't be created.
	 * 
	 * @param validationCheckId
	 * @param parameter
	 * @param connection
	 * @return <code>boolean</code> to indicate if the parameter was successfully
	 *         created
	 * @throws SQLException
	 */
	private static boolean createParameter(Long validationCheckId, String parameter, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_PARAMETER);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			Long newId = getLastParameterId(validationCheckId, connection) + 1;
			statement.setLong(1, validationCheckId);
			statement.setLong(2, newId);
			statement.setString(3, parameter);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			return created > 0;
		}
	}

	/**
	 * Get the last registered ID for a parameter related to a
	 * {@link ValidationCheck} ID
	 * 
	 * @param validationCheckId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static Long getLastParameterId(Long validationCheckId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LAST_PARAMETER_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, validationCheckId);
			ResultSet rs = statement.executeQuery();
			// First in the table
			if (!rs.next()) {
				return 0L;
			}
			return rs.getLong(1);
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		ValidationCheckModel.queryGroup = queryGroup;
	}
}
