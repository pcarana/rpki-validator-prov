package mx.nic.lab.rpki.prov.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.ValidationCheck;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.ValidationCheckDbObject;

/**
 * Model to retrieve Validation check data from the database
 *
 */
public class ValidationCheckModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(ValidationCheckModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "ValidationCheck";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_VALIDATION_RUN_ID = "getByValidationRunId";
	private static final String GET_PARAMETERS = "getParameters";
	private static final String GET_LAST_PARAMETER_ID = "getLastParameterId";
	private static final String GET_LAST_SUCCESSFUL_CHECKS_BY_TAL = "getLastSuccessfulChecksByTal";
	private static final String GET_LAST_SUCCESSFUL_CHECKS_BY_TAL_COUNT = "getLastSuccessfulChecksByTalCount";
	private static final String CREATE = "create";
	private static final String CREATE_PARAMETER = "createParameter";
	private static final String GET_LAST_ROWID = "getLastRowid";

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
	 * Get the {@link Class} to use as a lock
	 * 
	 * @return
	 */
	private static Class<ValidationCheckModel> getModelClass() {
		return ValidationCheckModel.class;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ValidationCheckDbObject stored = new ValidationCheckDbObject(newValidationCheck);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			if (created < 1) {
				return null;
			}
			newValidationCheck.setId(getLastRowid(connection));
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
	public static Set<ValidationCheck> getByValidationRunId(Long validationRunId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_VALIDATION_RUN_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, validationRunId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			Set<ValidationCheck> validationChecks = new HashSet<>();
			while (rs.next()) {
				ValidationCheckDbObject validationCheck = new ValidationCheckDbObject(rs);
				loadRelatedObjects(validationCheck, connection);
				validationChecks.add(validationCheck);
			}
			return validationChecks;
		}
	}

	/**
	 * Get the validation checks related to the last successful validation run of a
	 * TAL
	 * 
	 * @param talId
	 * @param pagingParams
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static ListResult<ValidationCheck> getLastSuccessfulChecksByTal(Long talId, PagingParameters pagingParams,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LAST_SUCCESSFUL_CHECKS_BY_TAL);
		query = Util.getQueryWithPaging(query, pagingParams, ValidationCheckDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 2);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<ValidationCheck> validationChecks = new ArrayList<ValidationCheck>();
			while (rs.next()) {
				ValidationCheckDbObject validationCheck = new ValidationCheckDbObject(rs);
				loadRelatedObjects(validationCheck, connection);
				validationChecks.add(validationCheck);
			}
			Integer totalFound = getAllChecksByTalCount(talId, pagingParams, connection);
			return new ListResult<ValidationCheck>(validationChecks, totalFound);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, validationCheckId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			Long newId = getLastParameterId(validationCheckId, connection) + 1;
			statement.setLong(1, validationCheckId);
			statement.setLong(2, newId);
			statement.setString(3, parameter);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
		}
	}

	/**
	 * Get the count of all the {@link ValidationCheck}s related to the last
	 * successful validation of a TAL, return 0 when no records are found
	 * 
	 * @param talId
	 * @param pagingParams
	 * @param connection
	 * @return The count of all {@link ValidationCheck}s, or 0 when no data is found
	 * @throws SQLException
	 */
	private static Integer getAllChecksByTalCount(Long talId, PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_LAST_SUCCESSFUL_CHECKS_BY_TAL_COUNT);
		query = Util.getQueryWithPaging(query, pagingParams, ValidationCheckDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 2);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return 0;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, validationCheckId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			// First in the table
			if (!rs.next()) {
				return 0L;
			}
			return rs.getLong(1);
		}
	}

	/**
	 * Get the last rowid used in an insert statement, using object sequence
	 * 
	 * @param connection
	 * @return the last inserted ID
	 * @throws SQLException
	 */
	private static Long getLastRowid(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LAST_ROWID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ResultSet resultSet = executeQuery(statement, getModelClass(), logger);
			if (!resultSet.next()) {
				return 1L;
			}
			return resultSet.getLong(1);
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		ValidationCheckModel.queryGroup = queryGroup;
	}
}
