package mx.nic.lab.rpki.sqlite.model;

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
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.RpkiRepository;
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.RpkiRepositoryDbObject;

/**
 * Model to retrieve RPKI Repositories data from the database
 *
 */
public class RpkiRepositoryModel {

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
	private static final String GET_ALL = "getAll";
	private static final String CREATE_TAL_RELATION = "createTalRelation";
	private static final String UPDATE_PARENT_REPOSITORY = "updateParentRepository";

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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			RpkiRepositoryDbObject stored = new RpkiRepositoryDbObject(newRpkiRepository);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			stored = getByUniqueFields(stored, connection);
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, uri);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
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
	 * Get all the {@link RpkiRepository}s, return empty list when no files are
	 * found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link RpkiRepository}s, or empty list when no data is
	 *         found
	 * @throws SQLException
	 */
	public static List<RpkiRepository> getAll(PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, RpkiRepositoryDbObject.propertyToColumnMap);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<RpkiRepository> rpkiRepositories = new ArrayList<RpkiRepository>();
			while (rs.next()) {
				RpkiRepositoryDbObject rpkiRepository = new RpkiRepositoryDbObject(rs);
				loadRelatedObjects(rpkiRepository, connection);
				rpkiRepositories.add(rpkiRepository);
			}

			return rpkiRepositories;
		}
	}

	public static Set<RpkiRepository> getByValidationRunId(Long validationRunId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_VALIDATION_RUN_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, validationRunId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			Set<RpkiRepository> rpkiRepositories = new HashSet<>();
			while (rs.next()) {
				RpkiRepositoryDbObject rpkiRepository = new RpkiRepositoryDbObject(rs);
				loadRelatedObjects(rpkiRepository, connection);
				rpkiRepositories.add(rpkiRepository);
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			if (rpkiRepository.getParentRepository() != null) {
				statement.setLong(1, rpkiRepository.getParentRepository().getId());
			} else {
				statement.setNull(1, Types.NUMERIC);
			}
			statement.setLong(2, rpkiRepository.getId());
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiRepositoryId);
			statement.setLong(2, talId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
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
		PreparedStatement statement = connection.prepareStatement(query);
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
			ResultSet resultSet = statement.executeQuery();
			RpkiRepositoryDbObject found = new RpkiRepositoryDbObject(resultSet);
			loadRelatedObjects(found, connection);
			return found;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RpkiRepositoryModel.queryGroup = queryGroup;
	}
}
