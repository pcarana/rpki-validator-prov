package mx.nic.lab.rpki.prov.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.RpkiRepository;
import mx.nic.lab.rpki.db.pojo.ValidationCheck;
import mx.nic.lab.rpki.db.pojo.ValidationRun;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.ValidationRunDbObject;

/**
 * Model to retrieve Validation runs data from the database
 *
 */
public class ValidationRunModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(ValidationRunModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "ValidationRun";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_BY_TAL_ID = "getByTalId";
	private static final String CREATE = "create";
	private static final String CREATE_REPOSITORY_RELATION = "createRepositoryRelation";
	private static final String DELETE_OLD = "deleteOld";
	private static final String UPDATE = "update";
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
	private static Class<ValidationRunModel> getModelClass() {
		return ValidationRunModel.class;
	}

	/**
	 * Get a {@link ValidationRun} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link ValidationRun} found
	 * @throws SQLException
	 */
	public static ValidationRun getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			ValidationRunDbObject validationRun = null;
			do {
				validationRun = new ValidationRunDbObject(rs);
			} while (rs.next());
			return validationRun;
		}
	}

	/**
	 * Get all the {@link ValidationRun}s related to a TAL, return empty list when
	 * no files are found
	 * 
	 * @param talId
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link ValidationRun}s, or empty list when no data is
	 *         found
	 * @throws SQLException
	 */
	public static List<ValidationRun> getByTalId(Long talId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_TAL_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<ValidationRun> validationRuns = new ArrayList<ValidationRun>();
			while (rs.next()) {
				ValidationRunDbObject validationRun = new ValidationRunDbObject(rs);
				validationRuns.add(validationRun);
				loadRelatedObjects(validationRun, connection);
			}
			return validationRuns;
		}
	}

	/**
	 * Delete the {@link ValidationRun}s that were completed before the one received
	 * 
	 * @param validationRun
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int deleteOldValidationRuns(ValidationRun validationRun, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_OLD);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, validationRun.getId());
			statement.setString(2, validationRun.getType().toString());
			statement.setLong(3, validationRun.getTalId());
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Creates a new {@link ValidationRun} returns null if the object couldn't be
	 * created.
	 * 
	 * @param newValidationRun
	 * @param connection
	 * @return The ID of the {@link ValidationRun} created
	 * @throws SQLException
	 */
	public static Long create(ValidationRun newValidationRun, Connection connection) throws SQLException {
		Long result = null;
		String query = getQueryGroup().getQuery(CREATE);
		boolean originalAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ValidationRunDbObject stored = new ValidationRunDbObject(newValidationRun);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			if (created > 0) {
				newValidationRun.setId(getLastRowid(connection));
				storeRelatedObjects(newValidationRun, connection);
				result = newValidationRun.getId();
			}
		} finally {
			connection.commit();
			connection.setAutoCommit(originalAutoCommit);
		}
		return result;
	}

	/**
	 * Updates a {@link ValidationRun} returns whether the operation was successful
	 * or not created.
	 * 
	 * @param validationRun
	 * @param connection
	 * @return <code>boolean</code> to indicate success
	 * @throws SQLException
	 */
	public static int completeValidation(ValidationRun validationRun, Connection connection) throws SQLException {
		int result = 0;
		String query = getQueryGroup().getQuery(UPDATE);
		boolean originalAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ValidationRunDbObject stored = new ValidationRunDbObject(validationRun);
			stored.storeToDatabase(statement);
			statement.setLong(statement.getParameterMetaData().getParameterCount(), validationRun.getId());
			int updated = executeUpdate(statement, getModelClass(), logger);
			storeRelatedObjects(validationRun, connection);
			result = updated;
		} finally {
			connection.commit();
			connection.setAutoCommit(originalAutoCommit);
		}
		return result;
	}

	/**
	 * Load the related objects to a {@link ValidationRun}
	 * 
	 * @param validationRun
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(ValidationRunDbObject validationRun, Connection connection)
			throws SQLException {
		validationRun.setRpkiRepositories(RpkiRepositoryModel.getByValidationRunId(validationRun.getId(), connection));
		validationRun.setValidationChecks(ValidationCheckModel.getByValidationRunId(validationRun.getId(), connection));
	}

	/**
	 * Store the related objects to a {@link ValidationRun}
	 * 
	 * @param validationRun
	 * @param connection
	 * @throws SQLException
	 */
	private static void storeRelatedObjects(ValidationRun validationRun, Connection connection) throws SQLException {
		Set<Long> rpkiRepositories = validationRun.getRpkiRepositories();
		if (rpkiRepositories != null) {
			for (Long rpkiRepositoryId : rpkiRepositories) {
				// The rpkiRepository is already stored at DB
				createRepositoriesRelation(validationRun.getId(), rpkiRepositoryId, connection);
			}
		}
		Set<ValidationCheck> validationChecks = validationRun.getValidationChecks();
		if (validationChecks != null) {
			for (ValidationCheck validationCheck : validationChecks) {
				// If a validation check with error/warning exist, ignore the passed checks at
				// the same location
				boolean create = true;
				if (validationCheck.getStatus() == ValidationCheck.Status.PASSED) {
					for (ValidationCheck check : validationChecks) {
						if (!check.equals(validationCheck) && check.getLocation() == validationCheck.getLocation()) {
							create = false;
							break;
						}
					}
				}
				if (create) {
					validationCheck.setValidationRunId(validationRun.getId());
					ValidationCheckModel.create(validationCheck, connection);
				}
			}
		}
	}

	/**
	 * Creates a new relation between {@link ValidationRun} and
	 * {@link RpkiRepository} returns false if the relation couldn't be created.
	 * 
	 * @param validationRunId
	 * @param rpkiRepositoryId
	 * @param connection
	 * @return <code>boolean</code> to indicate if the relation was successfully
	 *         created
	 * @throws SQLException
	 */
	private static boolean createRepositoriesRelation(Long validationRunId, Long rpkiRepositoryId,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_REPOSITORY_RELATION);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, validationRunId);
			statement.setLong(2, rpkiRepositoryId);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
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
		ValidationRunModel.queryGroup = queryGroup;
	}
}
