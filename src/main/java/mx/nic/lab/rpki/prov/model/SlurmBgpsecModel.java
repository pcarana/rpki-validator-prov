package mx.nic.lab.rpki.prov.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.SlurmBgpsec;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.SlurmBgpsecDbObject;

/**
 * Model to retrieve SLURM BGPsec data from the database
 *
 */
public class SlurmBgpsecModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(SlurmBgpsecModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "SlurmBgpsec";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_BY_PROPERTIES = "getByProperties";
	private static final String GET_ALL = "getAll";
	private static final String GET_ALL_BY_TYPE = "getAllByType";
	private static final String GET_ALL_COUNT = "getAllCount";
	private static final String GET_ALL_BY_TYPE_COUNT = "getAllByTypeCount";
	private static final String EXIST = "exist";
	private static final String CREATE = "create";
	private static final String DELETE_BY_ID = "deleteById";
	private static final String DELETE_ALL = "deleteAll";
	private static final String UPDATE_COMMENT = "updateComment";
	private static final String UPDATE_ORDER = "updateOrder";

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
	private static Class<SlurmBgpsecModel> getModelClass() {
		return SlurmBgpsecModel.class;
	}

	/**
	 * Get a {@link SlurmBgpsec} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link SlurmBgpsec} found
	 * @throws SQLException
	 */
	public static SlurmBgpsec getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			SlurmBgpsec slurmBgpsec = null;
			do {
				slurmBgpsec = new SlurmBgpsecDbObject(rs);
			} while (rs.next());

			return slurmBgpsec;
		}
	}

	/**
	 * Get all the {@link SlurmBgpsec}s found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The {@link ListResult} of {@link SlurmBgpsec}s found
	 * @throws SQLException
	 */
	public static ListResult<SlurmBgpsec> getAll(PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmBgpsecDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 1);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<SlurmBgpsec> slurmBgpsecs = new ArrayList<SlurmBgpsec>();
			while (rs.next()) {
				SlurmBgpsecDbObject slurmBgpsec = new SlurmBgpsecDbObject(rs);
				slurmBgpsecs.add(slurmBgpsec);
			}
			Integer totalFound = getAllCount(pagingParams, connection);
			return new ListResult<SlurmBgpsec>(slurmBgpsecs, totalFound);
		}
	}

	/**
	 * Get all the {@link SlurmBgpsec}s found by its type
	 * 
	 * @param type
	 * @param pagingParams
	 * @param connection
	 * @return The {@link ListResult} of {@link SlurmBgpsec}s found
	 * @throws SQLException
	 */
	public static ListResult<SlurmBgpsec> getAllByType(String type, PagingParameters pagingParams,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmBgpsecDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, type);
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 2);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<SlurmBgpsec> slurmBgpsecs = new ArrayList<SlurmBgpsec>();
			while (rs.next()) {
				SlurmBgpsecDbObject slurmBgpsec = new SlurmBgpsecDbObject(rs);
				slurmBgpsecs.add(slurmBgpsec);
			}
			Integer totalFound = getAllByTypeCount(type, pagingParams, connection);
			return new ListResult<SlurmBgpsec>(slurmBgpsecs, totalFound);
		}
	}

	/**
	 * Check if a {@link SlurmBgpsec} already exists
	 * 
	 * @param slurmBgpsec
	 * @param connection
	 * @return <code>boolean</code> to indicate if the object exists
	 * @throws SQLException
	 */
	public static boolean exist(SlurmBgpsec slurmBgpsec, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(EXIST);
		StringBuilder parameters = new StringBuilder();
		// The query already has a parameter "type"
		int currentIdx = 2;
		int asnIdx = -1;
		int skiIdx = -1;
		int publicKeyIdx = -1;
		if (slurmBgpsec.getAsn() != null) {
			parameters.append(" and ").append(SlurmBgpsecDbObject.ASN_COLUMN).append(" = ? ");
			asnIdx = currentIdx++;
		}
		if (slurmBgpsec.getSki() != null) {
			parameters.append(" and ").append(SlurmBgpsecDbObject.SKI_COLUMN).append(" = ? ");
			skiIdx = currentIdx++;
		}
		if (slurmBgpsec.getRouterPublicKey() != null) {
			parameters.append(" and ").append(SlurmBgpsecDbObject.ROUTER_PUBLIC_KEY_COLUMN).append(" = ? ");
			publicKeyIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, slurmBgpsec.getType());
			if (asnIdx > 0) {
				statement.setLong(asnIdx, slurmBgpsec.getAsn());
			}
			if (skiIdx > 0) {
				statement.setString(skiIdx, slurmBgpsec.getSki());
			}
			if (publicKeyIdx > 0) {
				statement.setString(publicKeyIdx, slurmBgpsec.getRouterPublicKey());
			}

			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link SlurmBgpsec} returns a <code>boolean</code> to indicate
	 * success
	 * 
	 * @param newSlurmBgpsec
	 * @param connection
	 * @return <code>boolean</code> to indicate success
	 * @throws SQLException
	 */
	public static boolean create(SlurmBgpsec newSlurmBgpsec, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			SlurmBgpsecDbObject stored = new SlurmBgpsecDbObject(newSlurmBgpsec);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
		}
	}

	/**
	 * Delete a {@link SlurmBgpsec} by its ID, returns the number of deleted records
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
	 * Delete all the {@link SlurmBgpsec}, returns the number of deleted records
	 * 
	 * @param connection
	 * @return <code>int</code> number of deleted records
	 * @throws SQLException
	 */
	public static int deleteAll(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_ALL);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Get a SLURM BGPsec by its distinctive properties (comment not included),
	 * return <code>null</code> if there's no match
	 * 
	 * @param asn
	 *            ASN
	 * @param ski
	 *            SKI
	 * @param routerPublicKey
	 *            Router public key (may be null)
	 * @param type
	 *            SLURM BGPsec type (filter or assertion)
	 * @param connection
	 * @return the {@link SlurmBgpsec} found or <code>null</code> if there's no
	 *         match
	 * @throws SQLException
	 */
	public static SlurmBgpsec getByProperties(Long asn, String ski, String routerPublicKey, String type,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_PROPERTIES);
		StringBuilder parameters = new StringBuilder();
		// The query already has a parameter "type"
		int currentIdx = 2;
		int asnIdx = -1;
		int skiIdx = -1;
		int routerPublicKeyIdx = -1;
		parameters.append(" and ").append(SlurmBgpsecDbObject.ASN_COLUMN);
		if (asn != null) {
			parameters.append(" = ? ");
			asnIdx = currentIdx++;
		} else {
			parameters.append(" is null ");
		}
		parameters.append(" and ").append(SlurmBgpsecDbObject.SKI_COLUMN);
		if (ski != null) {
			parameters.append(" = ? ");
			skiIdx = currentIdx++;
		} else {
			parameters.append(" is null ");
		}
		parameters.append(" and ").append(SlurmBgpsecDbObject.ROUTER_PUBLIC_KEY_COLUMN);
		if (routerPublicKey != null) {
			parameters.append(" = ? ");
			routerPublicKeyIdx = currentIdx++;
		} else {
			parameters.append(" is null ");
		}
		query = query.replace("[and]", parameters.toString());
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, type);
			if (asnIdx > 0) {
				statement.setLong(asnIdx, asn);
			}
			if (skiIdx > 0) {
				statement.setString(skiIdx, ski);
			}
			if (routerPublicKeyIdx > 0) {
				statement.setString(routerPublicKeyIdx, routerPublicKey);
			}
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			SlurmBgpsec slurmBgpsec = null;
			do {
				slurmBgpsec = new SlurmBgpsecDbObject(rs);
			} while (rs.next());

			return slurmBgpsec;
		}
	}

	/**
	 * Update the comment of a SLURM BGPsec
	 * 
	 * @param id
	 *            ID of the SLURM BGPsec
	 * @param newComment
	 *            New comment of the SLURM BGPsec
	 * @param connection
	 * @return Number of rows affected
	 * @throws SQLException
	 */
	public static int updateComment(Long id, String newComment, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(UPDATE_COMMENT);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			if (newComment != null) {
				statement.setString(1, newComment);
			} else {
				statement.setNull(1, Types.VARCHAR);
			}
			statement.setLong(2, id);
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Update the order of a SLURM BGPsec inside the JSON array at the SLURM file,
	 * corresponding to the type (filter/assertion)
	 * 
	 * @param id
	 *            ID of the SLURM BGPsec
	 * @param newOrder
	 *            New order of the SLURM BGPsec
	 * @param connection
	 * @return Number of rows affected
	 * @throws SQLException
	 */
	public static int updateOrder(Long id, int newOrder, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(UPDATE_ORDER);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			if (newOrder >= 0) {
				statement.setInt(1, newOrder);
			} else {
				statement.setNull(1, Types.INTEGER);
			}
			statement.setLong(2, id);
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Delete all the SLURM BGPsecs sent at the {@link Set} of IDs
	 * 
	 * @param ids
	 *            {@link Set} of IDs to delete
	 * @param connection
	 * @throws SQLException
	 */
	public static void bulkDelete(Set<Long> ids, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(DELETE_BY_ID);
		boolean originalAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			for (Long id : ids) {
				statement.setLong(1, id);
				executeUpdate(statement, getModelClass(), logger);
			}
		} finally {
			// Commit what has been done
			connection.commit();
			connection.setAutoCommit(originalAutoCommit);
		}
	}

	/**
	 * Get the count of all the {@link SlurmBgpsec}s, return 0 when no records are
	 * found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The count of all {@link SlurmBgpsec}s, or 0 when no data is found
	 * @throws SQLException
	 */
	private static Integer getAllCount(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_COUNT);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmBgpsecDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 1);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return 0;
		}
	}

	/**
	 * Get the count of all the {@link SlurmBgpsec}s by type, return 0 when no
	 * records are found
	 * 
	 * @param type
	 * @param pagingParams
	 * @param connection
	 * @return The count of all {@link SlurmBgpsec}s by type, or 0 when no data is
	 *         found
	 * @throws SQLException
	 */
	private static Integer getAllByTypeCount(String type, PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE_COUNT);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmBgpsecDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setString(1, type);
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 2);
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
		SlurmBgpsecModel.queryGroup = queryGroup;
	}
}
