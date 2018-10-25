package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.SlurmPrefixDbObject;

/**
 * Model to retrieve SLURM Prefix data from the database
 *
 */
public class SlurmPrefixModel extends DatabaseModel {

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
	private static final String GET_ALL_COUNT = "getAllCount";
	private static final String GET_ALL_BY_TYPE_COUNT = "getAllByTypeCount";
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
	 * Get the {@link Class} to use as a lock
	 * 
	 * @return
	 */
	private static Class<SlurmPrefixModel> getModelClass() {
		return SlurmPrefixModel.class;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
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
	 * Get all the {@link SlurmPrefix}s found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The {@link ListResult} of {@link SlurmPrefix}s found
	 * @throws SQLException
	 */
	public static ListResult<SlurmPrefix> getAll(PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmPrefixDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<SlurmPrefix> slurmPrefixes = new ArrayList<SlurmPrefix>();
			while (rs.next()) {
				SlurmPrefixDbObject slurmPrefix = new SlurmPrefixDbObject(rs);
				slurmPrefixes.add(slurmPrefix);
			}
			Integer totalFound = getAllCount(connection);
			return new ListResult<SlurmPrefix>(slurmPrefixes, totalFound);
		}
	}

	/**
	 * Get all the {@link SlurmPrefix}s found by its type
	 * 
	 * @param type
	 * @param pagingParams
	 * @param connection
	 * @return The {@link ListResult} of {@link SlurmPrefix}s found
	 * @throws SQLException
	 */
	public static ListResult<SlurmPrefix> getAllByType(int type, PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmPrefixDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setInt(1, type);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<SlurmPrefix> slurmPrefixes = new ArrayList<SlurmPrefix>();
			while (rs.next()) {
				SlurmPrefixDbObject slurmPrefix = new SlurmPrefixDbObject(rs);
				slurmPrefixes.add(slurmPrefix);
			}
			Integer totalFound = getAllByTypeCount(type, connection);
			return new ListResult<SlurmPrefix>(slurmPrefixes, totalFound);
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
		query = query.replace("[and]", parameters.toString());
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
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

			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link SlurmPrefix} returns a <code>boolean</code> to indicate
	 * success
	 * 
	 * @param newSlurmPrefix
	 * @param connection
	 * @return <code>boolean</code> to indicate success
	 * @throws SQLException
	 */
	public static boolean create(SlurmPrefix newSlurmPrefix, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			SlurmPrefixDbObject stored = new SlurmPrefixDbObject(newSlurmPrefix);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Get the count of all the {@link SlurmPrefix}es, return 0 when no records are
	 * found
	 * 
	 * @param connection
	 * @return The count of all {@link SlurmPrefix}es, or 0 when no data is found
	 * @throws SQLException
	 */
	private static Integer getAllCount(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_COUNT);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return 0;
		}
	}

	/**
	 * Get the count of all the {@link SlurmPrefix}es by type, return 0 when no
	 * records are found
	 * 
	 * @param connection
	 * @return The count of all {@link SlurmPrefix}es by type, or 0 when no data is
	 *         found
	 * @throws SQLException
	 */
	private static Integer getAllByTypeCount(int type, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE_COUNT);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setInt(1, type);
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
		SlurmPrefixModel.queryGroup = queryGroup;
	}
}
