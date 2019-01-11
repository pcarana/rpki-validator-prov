package mx.nic.lab.rpki.prov.model;

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
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.EncodedRpkiObject;
import mx.nic.lab.rpki.db.pojo.Gbr;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.db.pojo.RpkiObject.Type;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.EncodedRpkiObjectDbObject;
import mx.nic.lab.rpki.prov.object.RpkiObjectDbObject;
import mx.nic.lab.rpki.db.pojo.RpkiRepository;

/**
 * Model to retrieve RPKI Objects data from the database
 *
 */
public class RpkiObjectModel extends DatabaseModel {

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
	private static final String CREATE = "create";
	private static final String DELETE_UNREACHABLE = "deleteUnreachable";
	private static final String CREATE_ENCODED = "createEncodedRpkiObject";
	private static final String CREATE_LOCATION = "createLocation";
	private static final String CREATE_RPKI_REPO_REL = "createRpkiRepositoryRelation";
	private static final String DELETE_BY_RPKI_REPOSITORY_ID = "deleteByRpkiRepositoryId";
	private static final String GET_ID_BY_SHA256 = "getIdBySha256";
	private static final String UPDATE_LAST_REACH = "updateReached";

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
	private static Class<RpkiObjectModel> getModelClass() {
		return RpkiObjectModel.class;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, sha256);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RpkiObject rpkiObject = null;
			do {
				rpkiObject = new RpkiObjectDbObject(rs);
				rpkiObject.setRpkiRepositories(getRpkiRepositories(rpkiObject.getId(), connection));
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			int index = 1;
			for (byte[] sha256 : sha256Set) {
				statement.setBytes(index++, sha256);
			}
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<RpkiObject> rpkiObjects = new ArrayList<RpkiObject>();
			while (rs.next()) {
				RpkiObjectDbObject rpkiObject = new RpkiObjectDbObject(rs);
				rpkiObjects.add(rpkiObject);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, type.toString());
			statement.setBytes(2, authorityKeyIdentifier);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RpkiObjectDbObject rpkiObject = new RpkiObjectDbObject(rs);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, subjectKeyIdentifier);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RpkiObjectDbObject rpkiObject = new RpkiObjectDbObject(rs);
			return rpkiObject;
		}
	}

	/**
	 * Creates a Set of {@link RpkiObject}s using a DB transaction
	 * 
	 * @param rpkiObjects
	 * @param connection
	 * @throws SQLException
	 */
	public static void bulkCreate(Set<RpkiObject> rpkiObjects, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		boolean originalAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			for (RpkiObject newRpkiObject : rpkiObjects) {
				RpkiObjectDbObject stored = new RpkiObjectDbObject(newRpkiObject);
				stored.storeToDatabase(statement);
				executeUpdate(statement, getModelClass(), logger);
				newRpkiObject.setId(getIdBySha256(newRpkiObject.getSha256(), connection));
				storeRelatedObjects(newRpkiObject, connection);
			}
		} finally {
			// Commit what has been done
			connection.commit();
			connection.setAutoCommit(originalAutoCommit);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, unreachableSince.toString());
			return executeUpdate(statement, getModelClass(), logger);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiObjectId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiRepositoryId);
			statement.setLong(2, rpkiObjectId);
			return executeUpdate(statement, getModelClass(), logger);
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiRepositoryId);
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Updates the lastMarkedReachableAt field of the received {@link Set} of
	 * {@link RpkiObject}s
	 * 
	 * @param reachedObjects
	 * @param connection
	 * @return the number of affected rows
	 * @throws SQLException
	 */
	public static int updateReachedObjects(Set<RpkiObject> reachedObjects, Connection connection) throws SQLException {
		int result = 0;
		String query = getQueryGroup().getQuery(UPDATE_LAST_REACH);
		boolean originalAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			for (RpkiObject updRpkiObject : reachedObjects) {
				statement.setString(1, updRpkiObject.getLastMarkedReachableAt().toString());
				statement.setLong(2, updRpkiObject.getId());
				result += executeUpdate(statement, getModelClass(), logger);
			}
		} finally {
			// Commit what has been done
			connection.commit();
			connection.setAutoCommit(originalAutoCommit);
		}
		return result;
	}

	/**
	 * Get the locations of an {@link RpkiObject} based on its ID
	 * 
	 * @param rpkiObjectId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static SortedSet<String> getLocations(Long rpkiObjectId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LOCATIONS);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiObjectId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			SortedSet<String> result = new TreeSet<>();
			while (rs.next()) {
				result.add(rs.getString("rpo_locations"));
			}

			return result;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiObjectId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			Set<Long> result = new HashSet<>();
			while (rs.next()) {
				result.add(rs.getLong("rpr_id"));
			}
			return result;
		}
	}

	/**
	 * Store a {@link EncodedRpkiObject} and return <code>boolean</code> to indicate
	 * success
	 * 
	 * @param newEncodedRpkiObject
	 * @param connection
	 * @return <code>boolean</code> to indicate success
	 * @throws SQLException
	 */
	private static boolean createEncodedRpkiObject(EncodedRpkiObject newEncodedRpkiObject, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_ENCODED);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			EncodedRpkiObjectDbObject stored = new EncodedRpkiObjectDbObject(newEncodedRpkiObject);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiObjectId);
			statement.setString(2, location);
			int created = executeUpdate(statement, getModelClass(), logger);
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

	/**
	 * Get the ID of the object based on its SHA256 hash
	 * 
	 * @param sha256
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static Long getIdBySha256(byte[] sha256, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ID_BY_SHA256);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, sha256);
			ResultSet resultSet = executeQuery(statement, getModelClass(), logger);
			if (!resultSet.next()) {
				return null;
			}
			return resultSet.getLong(RpkiObjectDbObject.ID_COLUMN);
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RpkiObjectModel.queryGroup = queryGroup;
	}
}
