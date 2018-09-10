package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.db.pojo.RpkiRepository;
import mx.nic.lab.rpki.db.pojo.ValidationCheck;
import mx.nic.lab.rpki.db.pojo.ValidationRun;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.ValidationRunDbObject;

/**
 * Model to retrieve Validation runs data from the database
 *
 */
public class ValidationRunModel {

	private static final Logger logger = Logger.getLogger(ValidationRunModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "ValidationRun";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_ALL = "getAll";
	private static final String GET_LAST_ID = "getLastId";
	private static final String EXIST = "exist";
	private static final String CREATE = "create";
	private static final String CREATE_REPOSITORY_RELATION = "createRepositoryRelation";
	private static final String CREATE_RPKI_OBJ_RELATION = "createValidObjectsRelation";
	private static final String DELETE_OLD = "deleteOld";

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
	 * Get a {@link ValidationRun} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link ValidationRun} found
	 * @throws SQLException
	 */
	public static ValidationRun getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			ValidationRunDbObject validationRun = null;
			do {
				validationRun = new ValidationRunDbObject(rs);
				loadRelatedObjects(validationRun, connection);
			} while (rs.next());
			return validationRun;
		}
	}

	/**
	 * Get all the {@link ValidationRun}s, return empty list when no files are found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link ValidationRun}s, or empty list when no data is
	 *         found
	 * @throws SQLException
	 */
	public static List<ValidationRun> getAll(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, ValidationRunDbObject.propertyToColumnMap);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
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
	 * Delete the {@link ValidationRun}s that were completed before
	 * <code>completedBefore</code>
	 * 
	 * @param completedBefore
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int deleteOldValidationRuns(Instant completedBefore, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_OLD);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, completedBefore.toString());
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}

	/**
	 * Check if a {@link ValidationRun} already exists
	 * 
	 * @param validationRun
	 * @param connection
	 * @return <code>boolean</code> to indicate if the object exists
	 * @throws SQLException
	 */
	public static boolean exist(ValidationRun validationRun, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(EXIST);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int talIdIdx = -1;
		if (validationRun.getTal() != null) {
			parameters.append(" and ").append(ValidationRunDbObject.TAL_COLUMN).append(" = ? ");
			talIdIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			if (talIdIdx > 0) {
				statement.setLong(talIdIdx, validationRun.getTal().getId());
			}
			ResultSet rs = statement.executeQuery();
			return rs.next();
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
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			Long newId = getLastId(connection) + 1;
			newValidationRun.setId(newId);
			ValidationRunDbObject stored = new ValidationRunDbObject(newValidationRun);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			storeRelatedObjects(newValidationRun, connection);
			return newId;
		}
	}

	/**
	 * Get the last registered ID
	 * 
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static Long getLastId(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LAST_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			ResultSet rs = statement.executeQuery();
			// First in the table
			if (!rs.next()) {
				return 0L;
			}
			return rs.getLong(1);
		}
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
		validationRun.setTal(TalModel.getById(validationRun.getTalId(), connection));
		validationRun.setRpkiRepositories(RpkiRepositoryModel.getByValidationRunId(validationRun.getId(), connection));
		validationRun
				.setValidatedObjects(RpkiObjectModel.getValidatedByValidationRunId(validationRun.getId(), connection));
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
		// FIXME @pcarana What to do with validationRun.getRpkiObjects()
		// validationRun.getRpkiObjects()
		validationRun.getRpkiRepositories();
		Set<RpkiRepository> rpkiRepositories = validationRun.getRpkiRepositories();
		if (rpkiRepositories != null) {
			for (RpkiRepository rpkiRepository : rpkiRepositories) {
				// The rpkiRepository is already stored at DB
				createRepositoriesRelation(validationRun.getId(), rpkiRepository.getId(), connection);
			}
		}
		Set<RpkiObject> rpkiObjects = validationRun.getValidatedObjects();
		if (rpkiObjects != null) {
			for (RpkiObject rpkiObject : rpkiObjects) {
				// The rpkiObject is already stored at DB
				createValidObjectsRelation(validationRun.getId(), rpkiObject.getId(), connection);
			}
		}
		List<ValidationCheck> validationChecks = validationRun.getValidationChecks();
		if (validationChecks != null) {
			for (ValidationCheck validationCheck : validationChecks) {
				validationCheck.setValidationRun(validationRun);
				ValidationCheckModel.create(validationCheck, connection);
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, validationRunId);
			statement.setLong(2, rpkiRepositoryId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			return created > 0;
		}
	}

	/**
	 * Creates a new relation between {@link ValidationRun} and {@link RpkiObject}
	 * returns false if the relation couldn't be created.
	 * 
	 * @param validationRunId
	 * @param rpkiObjectId
	 * @param connection
	 * @return <code>boolean</code> to indicate if the relation was successfully
	 *         created
	 * @throws SQLException
	 */
	private static boolean createValidObjectsRelation(Long validationRunId, Long rpkiObjectId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_RPKI_OBJ_RELATION);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, validationRunId);
			statement.setLong(2, rpkiObjectId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			return created > 0;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		ValidationRunModel.queryGroup = queryGroup;
	}
}
