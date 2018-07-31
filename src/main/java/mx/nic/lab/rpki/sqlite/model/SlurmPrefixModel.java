package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.SlurmPrefixDbObject;

/**
 * Model to retrieve SLURM Prefix data from the database
 *
 */
public class SlurmPrefixModel {

	private static final Logger logger = Logger.getLogger(SlurmPrefixModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "SlurmPrefix";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_ALL = "getAll";
	private static final String GET_ALL_BY_TYPE = "getAllByType";
	private static final String GET_LAST_ID = "getLastId";
	private static final String EXIST = "exist";
	private static final String CREATE = "create";
	private static final String DELETE_BY_ID = "deleteById";

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
	 * Get a {@link SlurmPrefix} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link SlurmPrefix} found
	 * @throws SQLException
	 */
	public static SlurmPrefix getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			SlurmPrefix slurmPrefix = null;
			do {
				slurmPrefix = new SlurmPrefixDbObject(rs);
			} while (rs.next());

			return slurmPrefix;
		}
	}

	/**
	 * Get all the {@link SlurmPrefix}s, return empty list when no records are found
	 * 
	 * @param connection
	 * @return The list of {@link SlurmPrefix}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<SlurmPrefix> getAll(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<SlurmPrefix> slurmPrefixes = new ArrayList<SlurmPrefix>();
			do {
				SlurmPrefixDbObject slurmPrefix = new SlurmPrefixDbObject(rs);
				slurmPrefixes.add(slurmPrefix);
			} while (rs.next());

			return slurmPrefixes;
		}
	}

	/**
	 * Get all the {@link SlurmPrefix}s by its type, return empty list when no
	 * records are found
	 * 
	 * @param connection
	 * @param id
	 * @return The list of {@link SlurmPrefix}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<SlurmPrefix> getAllByType(Connection connection, int type) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setInt(1, type);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<SlurmPrefix> slurmPrefixes = new ArrayList<SlurmPrefix>();
			do {
				SlurmPrefixDbObject slurmPrefix = new SlurmPrefixDbObject(rs);
				slurmPrefixes.add(slurmPrefix);
			} while (rs.next());

			return slurmPrefixes;
		}
	}

	/**
	 * Check if a {@link SlurmPrefix} already exists
	 * 
	 * @param slurmPrefix
	 * @param connection
	 * @return <code>boolean</code> to indicate if the object exists
	 * @throws SQLException
	 */
	public static boolean exist(SlurmPrefix slurmPrefix, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(EXIST);
		StringBuilder parameters = new StringBuilder();
		// The query already has a parameter "type"
		int currentIdx = 2;
		int asnIdx = -1;
		int startPrefixIdx = -1;
		int prefixLengthIdx = -1;
		int prefixMaxLengthIdx = -1;
		if (slurmPrefix.getAsn() != null) {
			parameters.append(" and ").append(SlurmPrefixDbObject.ASN_COLUMN).append(" = ? ");
			asnIdx = currentIdx++;
		}
		if (slurmPrefix.getStartPrefix() != null) {
			parameters.append(" and ").append(SlurmPrefixDbObject.START_PREFIX_COLUMN).append(" = ? ");
			startPrefixIdx = currentIdx++;
		}
		if (slurmPrefix.getPrefixLength() != null) {
			parameters.append(" and ").append(SlurmPrefixDbObject.PREFIX_LENGTH_COLUMN).append(" = ? ");
			prefixLengthIdx = currentIdx++;
		}
		if (slurmPrefix.getPrefixMaxLength() != null) {
			parameters.append(" and ").append(SlurmPrefixDbObject.PREFIX_MAX_LENGTH_COLUMN).append(" = ? ");
			prefixMaxLengthIdx = currentIdx++;
		}
		query = query.replace("[where]", parameters.toString());
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setInt(1, slurmPrefix.getType());
			if (asnIdx > 0) {
				statement.setLong(asnIdx, slurmPrefix.getAsn());
			}
			if (startPrefixIdx > 0) {
				statement.setBytes(startPrefixIdx, slurmPrefix.getStartPrefix());
			}
			if (prefixLengthIdx > 0) {
				statement.setInt(prefixLengthIdx, slurmPrefix.getPrefixLength());
			}
			if (prefixMaxLengthIdx > 0) {
				statement.setInt(prefixMaxLengthIdx, slurmPrefix.getPrefixMaxLength());
			}

			ResultSet rs = statement.executeQuery();
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link SlurmPrefix} returns null if the object couldn't be
	 * created.
	 * 
	 * @param newSlurmPrefix
	 * @param connection
	 * @return The {@link SlurmPrefix} created
	 * @throws SQLException
	 */
	public static SlurmPrefix create(SlurmPrefix newSlurmPrefix, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			Long newId = getLastId(connection) + 1;
			newSlurmPrefix.setId(newId);
			SlurmPrefixDbObject stored = new SlurmPrefixDbObject(newSlurmPrefix);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			return getById(newId, connection);
		}
	}

	/**
	 * Delete a {@link SlurmPrefix} by its ID, returns the number of deleted records
	 * 
	 * @param id
	 * @param connection
	 * @return <code>int</code> number of deleted records
	 * @throws SQLException
	 */
	public static int deleteById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
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
				return 1L;
			}
			return rs.getLong(1);
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		SlurmPrefixModel.queryGroup = queryGroup;
	}
}
