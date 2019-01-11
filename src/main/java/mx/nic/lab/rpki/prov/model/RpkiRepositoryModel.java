package mx.nic.lab.rpki.prov.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.RpkiRepository;
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.RpkiRepositoryDbObject;

/**
 * Model to retrieve RPKI Repositories data from the database
 *
 */
public class RpkiRepositoryModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(RpkiRepositoryModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "RpkiRepository";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String CREATE = "create";
	private static final String GET_BY_UNIQUE = "getByUnique";
	private static final String GET_BY_ID = "getById";
	private static final String GET_BY_URI = "getByUri";
	private static final String GET_BY_VALIDATION_RUN_ID = "getByValidationRunId";
	private static final String CREATE_TAL_RELATION = "createTalRelation";
	private static final String UPDATE_PARENT_REPOSITORY = "updateParentRepository";
	private static final String GET_BY_TAL_ID = "getByTalId";
	private static final String GET_IDS_BY_TAL_ID = "getIdsByTalId";
	private static final String DELETE_BY_TAL_ID = "deleteByTalId";

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
	private static Class<RpkiRepositoryModel> getModelClass() {
		return RpkiRepositoryModel.class;
	}

	/**
	 * Creates a new {@link RpkiRepository} returns null if the object couldn't be
	 * created.
	 * 
	 * @param newRpkiRepository
	 * @param connection
	 * @return The ID of the {@link RpkiRepository} created
	 * @throws SQLException
	 */
	public static Long create(RpkiRepository newRpkiRepository, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			RpkiRepositoryDbObject stored = new RpkiRepositoryDbObject(newRpkiRepository);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			if (created < 1) {
				return null;
			}
			stored = getByUniqueFields(stored, connection);
			if (stored == null) {
				return null;
			}
			newRpkiRepository.setId(stored.getId());
			storeRelatedObjects(newRpkiRepository, connection);
			return newRpkiRepository.getId();
		}
	}

	/**
	 * Get a {@link RpkiRepository} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link RpkiRepository} found
	 * @throws SQLException
	 */
	public static RpkiRepository getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RpkiRepositoryDbObject rpkiRepository = null;
			do {
				rpkiRepository = new RpkiRepositoryDbObject(rs);
				loadRelatedObjects(rpkiRepository, connection);
			} while (rs.next());

			return rpkiRepository;
		}
	}

	/**
	 * Get a {@link RpkiRepository} by its URI, return null if no data is found
	 * 
	 * @param uri
	 * @param connection
	 * @return The {@link RpkiRepository} found
	 * @throws SQLException
	 */
	public static RpkiRepository getByUri(String uri, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_URI);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, uri);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RpkiRepositoryDbObject rpkiRepository = null;
			do {
				rpkiRepository = new RpkiRepositoryDbObject(rs);
				loadRelatedObjects(rpkiRepository, connection);
			} while (rs.next());

			return rpkiRepository;
		}
	}

	/**
	 * Get all the {@link RpkiRepository}s related to a TAL ID, return empty list
	 * when no files are found
	 * 
	 * @param talId
	 * @param connection
	 * @return The list of {@link RpkiRepository}s, or empty list when no data is
	 *         found
	 * @throws SQLException
	 */
	public static List<RpkiRepository> getByTalId(Long talId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_TAL_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<RpkiRepository> rpkiRepositories = new ArrayList<RpkiRepository>();
			while (rs.next()) {
				RpkiRepositoryDbObject rpkiRepository = new RpkiRepositoryDbObject(rs);
				loadRelatedObjects(rpkiRepository, connection);
				rpkiRepositories.add(rpkiRepository);
			}

			return rpkiRepositories;
		}
	}

	/**
	 * Get the IDs of the {@link RpkiRepository}s related to a ValidationRun ID
	 * 
	 * @param validationRunId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static Set<Long> getByValidationRunId(Long validationRunId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_VALIDATION_RUN_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, validationRunId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			Set<Long> rpkiRepositories = new HashSet<>();
			while (rs.next()) {
				RpkiRepositoryDbObject rpkiRepository = new RpkiRepositoryDbObject(rs);
				rpkiRepositories.add(rpkiRepository.getId());
			}
			return rpkiRepositories;
		}
	}

	/**
	 * Updates the parentRepository of the {@link RpkiRepository}
	 * 
	 * @param rpkiRepository
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int updateParentRepository(RpkiRepository rpkiRepository, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(UPDATE_PARENT_REPOSITORY);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			if (rpkiRepository.getParentRepository() != null) {
				statement.setLong(1, rpkiRepository.getParentRepository().getId());
			} else {
				statement.setNull(1, Types.NUMERIC);
			}
			statement.setLong(2, rpkiRepository.getId());
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Delete the {@link RpkiRepository}s related to a TAL ID
	 * 
	 * @param talId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int deleteByTalId(Long talId, Connection connection) throws SQLException {
		List<Long> rpkiRepositories = getRelatedIdsByTalId(talId, connection);
		for (Long rpkiRepositoryId : rpkiRepositories) {
			RpkiObjectModel.deleteByRpkiRepositoryId(rpkiRepositoryId, connection);
		}
		String query = getQueryGroup().getQuery(DELETE_BY_TAL_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Load the related objects to an {@link RpkiRepository}
	 * 
	 * @param rpkiRepository
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(RpkiRepositoryDbObject rpkiRepository, Connection connection)
			throws SQLException {
		if (rpkiRepository.getParentRepositoryId() != null) {
			rpkiRepository.setParentRepository(getById(rpkiRepository.getParentRepositoryId(), connection));
		}
		Set<Tal> relatedTals = TalModel.getByRpkiRepositoryId(rpkiRepository.getId(), connection);
		rpkiRepository.setTrustAnchors(relatedTals);
	}

	/**
	 * Store the related objects to an {@link RpkiRepository}
	 * 
	 * @param rpkiRepository
	 * @param connection
	 * @throws SQLException
	 */
	private static void storeRelatedObjects(RpkiRepository rpkiRepository, Connection connection) throws SQLException {
		for (Tal tal : rpkiRepository.getTrustAnchors()) {
			// The TAL already exists, store the relation
			createTalRelation(rpkiRepository.getId(), tal.getId(), connection);
		}
	}

	/**
	 * Creates a new relation between {@link RpkiRepository} and {@link Tal} returns
	 * false if the relation couldn't be created.
	 * 
	 * @param rpkiRepositoryId
	 * @param talId
	 * @param connection
	 * @return <code>boolean</code> to indicate if the relation was successfully
	 *         created
	 * @throws SQLException
	 */
	private static boolean createTalRelation(Long rpkiRepositoryId, Long talId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_TAL_RELATION);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiRepositoryId);
			statement.setLong(2, talId);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
		}
	}

	/**
	 * Return a {@link PreparedStatement} that contains the necessary parameters to
	 * make a search of a unique {@link RpkiRepository} based on properties distinct
	 * that the ID
	 * 
	 * @param rpkiRepository
	 * @param queryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement prepareUniqueSearch(RpkiRepository rpkiRepository, String queryId,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(queryId);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int locationUriIdx = -1;
		if (rpkiRepository.getLocationUri() != null) {
			parameters.append(" and ").append(RpkiRepositoryDbObject.LOCATION_URI_COLUMN).append(" = ? ");
			locationUriIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		PreparedStatement statement = prepareStatement(connection, query, getModelClass());
		if (locationUriIdx > 0) {
			statement.setString(locationUriIdx, rpkiRepository.getLocationUri());
		}
		return statement;
	}

	/**
	 * Get a {@link RpkiRepository} by its unique fields
	 * 
	 * @param rpkiRepository
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static RpkiRepositoryDbObject getByUniqueFields(RpkiRepository rpkiRepository, Connection connection)
			throws SQLException {
		try (PreparedStatement statement = prepareUniqueSearch(rpkiRepository, GET_BY_UNIQUE, connection)) {
			ResultSet resultSet = executeQuery(statement, getModelClass(), logger);
			if (!resultSet.next()) {
				return null;
			}
			RpkiRepositoryDbObject found = new RpkiRepositoryDbObject(resultSet);
			loadRelatedObjects(found, connection);
			return found;
		}
	}

	/**
	 * Get the list of Rpki repositories IDs related to a TAL ID
	 * 
	 * @param talId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static List<Long> getRelatedIdsByTalId(Long talId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_IDS_BY_TAL_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<Long> rpkiRepositories = new ArrayList<>();
			while (rs.next()) {
				rpkiRepositories.add(rs.getLong(RpkiRepositoryDbObject.ID_COLUMN));
			}
			return rpkiRepositories;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RpkiRepositoryModel.queryGroup = queryGroup;
	}
}
