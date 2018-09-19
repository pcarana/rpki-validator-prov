package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.EncodedRpkiObject;
import mx.nic.lab.rpki.db.pojo.Gbr;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.db.pojo.RpkiObject.Type;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.EncodedRpkiObjectDbObject;
import mx.nic.lab.rpki.sqlite.object.RpkiObjectDbObject;

/**
 * Model to retrieve RPKI Objects data from the database
 *
 */
public class RpkiObjectModel {

	private static final Logger logger = Logger.getLogger(RpkiObjectModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "RpkiObject";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY = "getBy";
	private static final String GET_LOCATIONS = "getLocations";
	private static final String GET_ENCODED_BY_OBJECT_ID = "getEncodedByRpkiObjectId";
	private static final String GET_RPKI_REPO_REL = "getRpkiRepositoryRelation";
	private static final String EXIST = "exist";
	private static final String CREATE = "create";
	private static final String DELETE = "delete";
	private static final String DELETE_UNREACHABLE = "deleteUnreachable";
	private static final String CREATE_ENCODED = "createEncodedRpkiObject";
	private static final String GET_ENCODED_LAST_ID = "getEncodedRpkiObjectLastId";
	private static final String CREATE_LOCATION = "createLocation";
	private static final String CREATE_RPKI_REPO_REL = "createRpkiRepositoryRelation";
	private static final String DELETE_BY_RPKI_REPOSITORY_ID = "deleteByRpkiRepositoryId";

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
	 * Get a {@link RpkiObject} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link RpkiObject} found
	 * @throws SQLException
	 */
	public static RpkiObject getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY);
		StringBuilder parameters = new StringBuilder();
		parameters.append(" and ").append(RpkiObjectDbObject.ID_COLUMN).append(" = ? ");
		query = query.replace("[and]", parameters.toString());
		query = Util.getQueryWithPaging(query, null, null);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			RpkiObject rpkiObject = null;
			do {
				rpkiObject = new RpkiObjectDbObject(rs);
				loadRelatedObjects(rpkiObject, connection);
			} while (rs.next());

			return rpkiObject;
		}
	}

	/**
	 * Get a {@link RpkiObject} by its SHA256, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link RpkiObject} found
	 * @throws SQLException
	 */
	public static RpkiObject getBySha256(byte[] sha256, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY);
		StringBuilder parameters = new StringBuilder();
		parameters.append(" and ").append(RpkiObjectDbObject.SHA256_COLUMN).append(" = ? ");
		query = query.replace("[and]", parameters.toString());
		query = Util.getQueryWithPaging(query, null, null);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setBytes(1, sha256);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			RpkiObject rpkiObject = null;
			do {
				rpkiObject = new RpkiObjectDbObject(rs);
				loadRelatedObjects(rpkiObject, connection);
			} while (rs.next());

			return rpkiObject;
		}
	}

	/**
	 * Get all the {@link RpkiObject}s whom sha256 matches the received set, return
	 * empty list when no files are found
	 * 
	 * @param sha256Set
	 * @param connection
	 * @return The list of {@link RpkiObject}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<RpkiObject> getBySha256Set(Set<byte[]> sha256Set, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY);
		StringBuilder parameters = new StringBuilder();
		parameters.append(" and ").append(RpkiObjectDbObject.SHA256_COLUMN).append(" in (");
		for (int i = 0; i < sha256Set.size(); i++) {
			parameters.append("?");
			if (i < sha256Set.size() - 1) {
				parameters.append(", ");
			}
		}
		parameters.append(") ");
		query = query.replace("[and]", parameters.toString());
		query = Util.getQueryWithPaging(query, null, null);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			int index = 1;
			for (byte[] sha256 : sha256Set) {
				statement.setBytes(index++, sha256);
			}
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<RpkiObject> rpkiObjects = new ArrayList<RpkiObject>();
			while (rs.next()) {
				RpkiObjectDbObject rpkiObject = new RpkiObjectDbObject(rs);
				loadRelatedObjects(rpkiObject, connection);
			}
			return rpkiObjects;
		}
	}

	/**
	 * Get the newest created {@link RpkiObject} that matches the <code>type</code>
	 * and <code>authorityKeyIdentifier</code> received
	 * 
	 * @param type
	 * @param authorityKeyIdentifier
	 * @param pagingParams
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static RpkiObject getLatestByTypeAndAuthorityKeyIdentifier(Type type, byte[] authorityKeyIdentifier,
			PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY);
		StringBuilder parameters = new StringBuilder();
		parameters.append(" and ").append(RpkiObjectDbObject.TYPE_COLUMN).append(" = ? ");
		parameters.append(" and ").append(RpkiObjectDbObject.AUTHORITY_KEY_IDENTIFIER_COLUMN).append(" = ? ");
		query = query.replace("[and]", parameters.toString());
		query = Util.getQueryWithPaging(query, pagingParams, RpkiObjectDbObject.propertyToColumnMap);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, type.toString());
			statement.setBytes(2, authorityKeyIdentifier);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			RpkiObjectDbObject rpkiObject = new RpkiObjectDbObject(rs);
			loadRelatedObjects(rpkiObject, connection);
			return rpkiObject;
		}
	}

	/**
	 * Get a {@link RpkiObject} by its SubjectKeyIdentifier, return null if the
	 * object isn't found
	 * 
	 * @param subjectKeyIdentifier
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static RpkiObject getBySubjectKeyIdentifier(byte[] subjectKeyIdentifier, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY);
		StringBuilder parameters = new StringBuilder();
		parameters.append(" and ").append(RpkiObjectDbObject.SUBJECT_KEY_IDENTIFIER_COLUMN).append(" = ? ");
		query = query.replace("[and]", parameters.toString());
		query = Util.getQueryWithPaging(query, null, null);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setBytes(1, subjectKeyIdentifier);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			RpkiObjectDbObject rpkiObject = new RpkiObjectDbObject(rs);
			loadRelatedObjects(rpkiObject, connection);
			return rpkiObject;
		}
	}

	/**
	 * Check if a {@link RpkiObject} already exists
	 * 
	 * @param rpkiObject
	 * @param connection
	 * @return <code>boolean</code> to indicate if the object exists
	 * @throws SQLException
	 */
	public static boolean exist(RpkiObject rpkiObject, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(EXIST);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int typeIdx = -1;
		int sha256Idx = -1;
		if (rpkiObject.getType() != null) {
			parameters.append(" and ").append(RpkiObjectDbObject.TYPE_COLUMN).append(" = ? ");
			typeIdx = currentIdx++;
		}
		if (rpkiObject.getSha256() != null) {
			parameters.append(" and ").append(RpkiObjectDbObject.SHA256_COLUMN).append(" = ? ");
			sha256Idx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			if (typeIdx > 0) {
				statement.setString(typeIdx, rpkiObject.getType().toString());
			}
			if (sha256Idx > 0) {
				statement.setBytes(sha256Idx, rpkiObject.getSha256());
			}

			ResultSet rs = statement.executeQuery();
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link RpkiObject} returns null if the object couldn't be
	 * created.
	 * 
	 * @param newRpkiObject
	 * @param connection
	 * @return The ID of the {@link RpkiObject} created
	 * @throws SQLException
	 */
	public static Long create(RpkiObject newRpkiObject, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			RpkiObjectDbObject stored = new RpkiObjectDbObject(newRpkiObject);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			stored = (RpkiObjectDbObject) getBySha256(newRpkiObject.getSha256(), connection);
			newRpkiObject.setId(stored.getId());
			storeRelatedObjects(newRpkiObject, connection);
			return newRpkiObject.getId();
		}
	}

	/**
	 * Delete the {@link RpkiObject}, this assumes that the DB has a "ON DELETE
	 * CASCADE" related constraint
	 * 
	 * @param rpkiObject
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int delete(RpkiObject rpkiObject, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiObject.getId());
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}

	/**
	 * Delete the {@link RpkiObject} that aren't reachable since
	 * <code>unreachableSince</code>, this assumes that the DB has a "ON DELETE
	 * CASCADE" related constraint
	 * 
	 * @param unreachableSince
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int deleteUnreachableObjects(Instant unreachableSince, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_UNREACHABLE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, unreachableSince.toString());
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}

	/**
	 * Get an {@link EncodedRpkiObject} related to an {@link RpkiObject} based on
	 * its ID
	 * 
	 * @param rpkiObjectId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static EncodedRpkiObject getEncodedByRpkiObjectId(Long rpkiObjectId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ENCODED_BY_OBJECT_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiObjectId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			return new EncodedRpkiObjectDbObject(rs);
		}
	}

	/**
	 * Create the relation between an {@link RpkiObject} and a
	 * {@link RpkiRepository} based on its ID
	 * 
	 * @param rpkiObjectId
	 * @param rpkiRepositoryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int addRpkiRepository(Long rpkiObjectId, Long rpkiRepositoryId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_RPKI_REPO_REL);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiRepositoryId);
			statement.setLong(2, rpkiObjectId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}

	/**
	 * Delete the objects related to a Rpki repository
	 * 
	 * @param rpkiRepositoryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static int deleteByRpkiRepositoryId(Long rpkiRepositoryId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_BY_RPKI_REPOSITORY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiRepositoryId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}

	/**
	 * Get the last registered ID for an EncodedRpkiObject
	 * 
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static Long getEncodedObjectLastId(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ENCODED_LAST_ID);
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
	 * Store the related objects to an {@link RpkiObject}
	 * 
	 * @param rpkiObject
	 * @param connection
	 * @throws SQLException
	 */
	private static void storeRelatedObjects(RpkiObject rpkiObject, Connection connection) throws SQLException {
		// ROAs
		for (Roa roa : rpkiObject.getRoas()) {
			roa.setRpkiObject(rpkiObject);
			RoaModel.create(roa, connection);
		}
		// EncodedRpkiObject
		EncodedRpkiObject encObject = rpkiObject.getEncodedRpkiObject();
		encObject.setRpkiObject(rpkiObject);
		createEncodedRpkiObject(encObject, connection);
		// Rpki Repositories relation
		createRpkiRepositoryRelation(rpkiObject.getRpkiRepositories(), rpkiObject.getId(), connection);
		// Locations
		Iterator<String> it = rpkiObject.getLocations().iterator();
		while (it.hasNext()) {
			createLocation(rpkiObject.getId(), it.next(), connection);
		}
		// Ghostbuster Record
		if (rpkiObject.getGbr() != null) {
			Gbr gbr = rpkiObject.getGbr();
			gbr.setRpkiObject(rpkiObject);
			GbrModel.create(gbr, connection);
		}
	}

	/**
	 * Load the related objects {@link RpkiObject}
	 * 
	 * @param rpkiObject
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(RpkiObject rpkiObject, Connection connection) throws SQLException {
		Long id = rpkiObject.getId();
		rpkiObject.setEncodedRpkiObject(getEncodedByRpkiObjectId(id, connection));
		rpkiObject.setRpkiRepositories(getRpkiRepositories(id, connection));
		rpkiObject.setLocations(getLocations(id, connection));
		// The ROAs and GBR aren't loaded
	}

	/**
	 * Get the RPKI repositories IDs related to an {@link RpkiObject}
	 * 
	 * @param rpkiObjectId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static Set<Long> getRpkiRepositories(Long rpkiObjectId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_RPKI_REPO_REL);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiObjectId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			Set<Long> result = new HashSet<>();
			while (rs.next()) {
				result.add(rs.getLong("rpr_id"));
			}
			return result;
		}
	}

	/**
	 * Get the locations of an {@link RpkiObject} based on its ID
	 * 
	 * @param rpkiObjectId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static SortedSet<String> getLocations(Long rpkiObjectId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LOCATIONS);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiObjectId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			SortedSet<String> result = new TreeSet<>();
			while (rs.next()) {
				result.add(rs.getString("rpo_locations"));
			}

			return result;
		}
	}

	/**
	 * Store a {@link EncodedRpkiObject} and return the ID of the created object
	 * 
	 * @param newEncodedRpkiObject
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static Long createEncodedRpkiObject(EncodedRpkiObject newEncodedRpkiObject, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_ENCODED);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			Long newId = getEncodedObjectLastId(connection) + 1;
			newEncodedRpkiObject.setId(newId);
			EncodedRpkiObjectDbObject stored = new EncodedRpkiObjectDbObject(newEncodedRpkiObject);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			return newId;
		}
	}

	/**
	 * Create a location related to an {@link RpkiObject}
	 * 
	 * @param rpkiObjectId
	 * @param location
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static boolean createLocation(Long rpkiObjectId, String location, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_LOCATION);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiObjectId);
			statement.setString(2, location);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			return created > 0;
		}
	}

	/**
	 * Create the relation between an {@link RpkiObject} and a set of
	 * {@link RpkiRepository}s
	 * 
	 * @param rpkiRepositories
	 * @param rpkiObjectId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static boolean createRpkiRepositoryRelation(Set<Long> rpkiRepositories, Long rpkiObjectId,
			Connection connection) throws SQLException {
		int created = 0;
		for (Long rpkiRepositoryId : rpkiRepositories) {
			created += addRpkiRepository(rpkiObjectId, rpkiRepositoryId, connection);
		}
		return created == rpkiRepositories.size();
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RpkiObjectModel.queryGroup = queryGroup;
	}
}
