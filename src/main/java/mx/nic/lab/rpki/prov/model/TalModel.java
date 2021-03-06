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
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.db.pojo.TalUri;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.TalDbObject;

/**
 * Model to retrieve TALs data from the database
 *
 */
public class TalModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(TalModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "Tal";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_BY_RPKI_REPO_ID = "getByRpkiRepositoryId";
	private static final String GET_ALL = "getAll";
	private static final String GET_ALL_COUNT = "getAllCount";
	private static final String GET_BY_UNIQUE = "getByUnique";
	private static final String EXIST = "exist";
	private static final String CREATE = "create";
	private static final String DELETE = "delete";
	private static final String UPDATE_LOADED_CER = "updateLoadedCer";

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
	private static Class<TalModel> getModelClass() {
		return TalModel.class;
	}

	/**
	 * Get a {@link Tal} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link Tal} found
	 * @throws SQLException
	 */
	public static Tal getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			Tal tal = null;
			do {
				tal = new TalDbObject(rs);
				loadRelatedObjects(tal, true, connection);
			} while (rs.next());

			return tal;
		}
	}

	/**
	 * Get all the {@link Tal}s found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The {@link ListResult} of {@link Tal}s found
	 * @throws SQLException
	 */
	public static ListResult<Tal> getAll(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, TalDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 1);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<Tal> tals = new ArrayList<Tal>();
			while (rs.next()) {
				TalDbObject tal = new TalDbObject(rs);
				loadRelatedObjects(tal, true, connection);
				tals.add(tal);
			}
			Integer totalFound = getAllCount(pagingParams, connection);
			return new ListResult<Tal>(tals, totalFound);
		}
	}

	/**
	 * Get an existent {@link Tal} by its unique fields, return null if no data is
	 * found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link Tal} found
	 * @throws SQLException
	 */
	public static Tal getExistentTal(Tal tal, Connection connection) throws SQLException {
		try (PreparedStatement statement = prepareUniqueSearch(tal, GET_BY_UNIQUE, connection)) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			Tal found = null;
			do {
				found = new TalDbObject(rs);
				loadRelatedObjects(found, false, connection);
			} while (rs.next());

			return found;
		}
	}

	/**
	 * Check if a {@link Tal} already exists
	 * 
	 * @param tal
	 * @param connection
	 * @return <code>boolean</code> to indicate if the object exists
	 * @throws SQLException
	 */
	public static boolean exist(Tal tal, Connection connection) throws SQLException {
		try (PreparedStatement statement = prepareUniqueSearch(tal, EXIST, connection)) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link Tal} returns null if the object couldn't be created.
	 * 
	 * @param newTal
	 * @param connection
	 * @return The ID of the {@link Tal} created
	 * @throws SQLException
	 */
	public static Long create(Tal newTal, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			TalDbObject stored = new TalDbObject(newTal);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			if (created < 1) {
				return null;
			}
			stored = getByUniqueFields(stored, connection);
			if (stored == null) {
				return null;
			}
			newTal.setId(stored.getId());
			storeRelatedObjects(newTal, connection);
			return newTal.getId();
		}
	}

	/**
	 * Delete a {@link Tal}, this assumes that the DB has a "ON DELETE CASCADE"
	 * related constraint
	 * 
	 * @param tal
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int delete(Tal tal, Connection connection) throws SQLException {
		// Delete the related repositories and objects
		RpkiRepositoryModel.deleteByTalId(tal.getId(), connection);
		String query = getQueryGroup().getQuery(DELETE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, tal.getId());
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Get the related {@link Tal}s to an RPKI repository based on its ID
	 * 
	 * @param rpkiRepositoryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static Set<Tal> getByRpkiRepositoryId(Long rpkiRepositoryId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_RPKI_REPO_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiRepositoryId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			Set<Tal> tals = new HashSet<>();
			while (rs.next()) {
				TalDbObject tal = new TalDbObject(rs);
				loadRelatedObjects(tal, false, connection);
				tals.add(tal);
			}
			return tals;
		}
	}

	/**
	 * Update a {@link Tal} loaded certificate
	 * 
	 * @param tal
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int updateLoadedCertificate(Tal tal, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(UPDATE_LOADED_CER);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, tal.getLoadedCer());
			statement.setLong(2, tal.getId());
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Load all the related objects to the TAL
	 * 
	 * @param tal
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(Tal tal, boolean loadValidationRuns, Connection connection)
			throws SQLException {
		Long talId = tal.getId();
		if (loadValidationRuns) {
			tal.setValidationRuns(ValidationRunModel.getByTalId(talId, connection));
		}
		tal.setTalUris(TalUriModel.getByTalId(talId, connection));
	}

	/**
	 * Store the related objects to a {@link Tal}
	 * 
	 * @param tal
	 * @param connection
	 * @throws SQLException
	 */
	private static void storeRelatedObjects(Tal tal, Connection connection) throws SQLException {
		for (TalUri talUri : tal.getTalUris()) {
			talUri.setTalId(tal.getId());
			TalUriModel.create(talUri, connection);
		}
	}

	/**
	 * Return a {@link PreparedStatement} that contains the necessary parameters to
	 * make a search of a unique {@link Tal} based on properties distinct that the
	 * ID
	 * 
	 * @param tal
	 * @param queryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement prepareUniqueSearch(Tal tal, String queryId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(queryId);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int publicKeyIdx = -1;
		if (tal.getPublicKey() != null) {
			parameters.append(" and ").append(TalDbObject.PUBLIC_KEY_COLUMN).append(" = ? ");
			publicKeyIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		PreparedStatement statement = prepareStatement(connection, query, getModelClass());
		if (publicKeyIdx > 0) {
			statement.setString(publicKeyIdx, tal.getPublicKey());
		}
		return statement;
	}

	/**
	 * Get a {@link Tal} by its unique fields
	 * 
	 * @param tal
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static TalDbObject getByUniqueFields(Tal tal, Connection connection) throws SQLException {
		try (PreparedStatement statement = prepareUniqueSearch(tal, GET_BY_UNIQUE, connection)) {
			ResultSet resultSet = executeQuery(statement, getModelClass(), logger);
			if (!resultSet.next()) {
				return null;
			}
			TalDbObject found = new TalDbObject(resultSet);
			loadRelatedObjects(found, false, connection);
			return found;
		}
	}

	/**
	 * Get the count of all the {@link Tal}s, return 0 when no records are found
	 * 
	 * @param connection
	 * @return The count of all {@link Tal}s, or 0 when no data is found
	 * @throws SQLException
	 */
	private static Integer getAllCount(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_COUNT);
		query = Util.getQueryWithPaging(query, pagingParams, TalDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			Util.setFilterParam(pagingParams, statement, 1);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return 0;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		TalModel.queryGroup = queryGroup;
	}
}
