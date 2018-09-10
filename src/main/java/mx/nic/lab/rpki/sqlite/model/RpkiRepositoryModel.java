package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
	private static final String GET_BY_ID = "getById";
	private static final String GET_BY_URI = "getByUri";
	private static final String GET_BY_VALIDATION_RUN_ID = "getByValidationRunId";
	private static final String GET_ALL = "getAll";

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

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RpkiRepositoryModel.queryGroup = queryGroup;
	}
}
