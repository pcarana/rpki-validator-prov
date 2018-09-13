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
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.db.pojo.TalUri;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.TalDbObject;

/**
 * Model to retrieve TALs data from the database
 *
 */
public class TalModel {

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
	private static final String SYNC_BY_ID = "syncById";
	private static final String GET_LAST_ID = "getLastId";
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
	 * Get a {@link Tal} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link Tal} found
	 * @throws SQLException
	 */
	public static Tal getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			Tal tal = null;
			do {
				tal = new TalDbObject(rs);
				loadRelatedObjects(tal, connection);
			} while (rs.next());

			return tal;
		}
	}

	/**
	 * Get all the {@link Tal}s, return empty list when no files are found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link Tal}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<Tal> getAll(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, TalDbObject.propertyToColumnMap);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<Tal> tals = new ArrayList<Tal>();
			while (rs.next()) {
				TalDbObject tal = new TalDbObject(rs);
				loadRelatedObjects(tal, connection);
				tals.add(tal);
			}
			return tals;
		}
	}

	/**
	 * Sync a {@link Tal} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link Tal} that will be synchronized
	 * @throws SQLException
	 */
	public static Tal syncById(Long id, Connection connection) throws SQLException {
		// First get the TAL and check status
		Tal tal = TalModel.getById(id, connection);
		if (tal == null) {
			return null;
		}
		if (Tal.SyncStatus.SYNCHRONIZING.toString().equalsIgnoreCase(tal.getSyncStatus())) {
			return tal;
		}
		String query = getQueryGroup().getQuery(SYNC_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int result = statement.executeUpdate();
			if (result == 0) {
				return null;
			}

			return TalModel.getById(id, connection);
		}
	}

	/**
	 * Sync all the {@link Tal}s, return empty list when no files are found nor
	 * could be synchronized
	 * 
	 * @param connection
	 * @return The list of {@link Tal}s, or empty list when no data is found nor
	 *         could be synchronized
	 * @throws SQLException
	 */
	public static List<Tal> syncAll(Connection connection) throws SQLException {
		List<Tal> loadedTals = TalModel.getAll(null, connection);
		List<Tal> syncdTals = new ArrayList<Tal>();
		for (Tal loadedTal : loadedTals) {
			Tal syncdTal = TalModel.syncById(loadedTal.getId(), connection);
			if (syncdTal != null) {
				syncdTals.add(syncdTal);
			}
		}
		return syncdTals;
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
		String query = getQueryGroup().getQuery(EXIST);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int publicKeyIdx = -1;
		int nameIdx = -1;
		if (tal.getPublicKey() != null) {
			parameters.append(" and ").append(TalDbObject.PUBLIC_KEY_COLUMN).append(" = ? ");
			publicKeyIdx = currentIdx++;
		}
		if (tal.getName() != null) {
			parameters.append(" and ").append(TalDbObject.NAME_COLUMN).append(" = ? ");
			nameIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			if (publicKeyIdx > 0) {
				statement.setString(publicKeyIdx, tal.getPublicKey());
			}
			if (nameIdx > 0) {
				statement.setString(nameIdx, tal.getName());
			}

			ResultSet rs = statement.executeQuery();
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			Long newId = getLastId(connection) + 1;
			newTal.setId(newId);
			TalDbObject stored = new TalDbObject(newTal);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			storeRelatedObjects(newTal, connection);
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
	 * Delete a {@link Tal}, this assumes that the DB has a "ON DELETE CASCADE"
	 * related constraint
	 * 
	 * @param tal
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int delete(Tal tal, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, tal.getId());
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiRepositoryId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			Set<Tal> tals = new HashSet<>();
			while (rs.next()) {
				TalDbObject tal = new TalDbObject(rs);
				loadRelatedObjects(tal, connection);
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setBytes(1, tal.getLoadedCer());
			statement.setLong(2, tal.getId());
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}
	
	/**
	 * Load all the related objects to the TAL
	 * 
	 * @param tal
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(Tal tal, Connection connection) throws SQLException {
		Long talId = tal.getId();
		tal.setTalFiles(TalFileModel.getByTalId(talId, connection));
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

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		TalModel.queryGroup = queryGroup;
	}
}
