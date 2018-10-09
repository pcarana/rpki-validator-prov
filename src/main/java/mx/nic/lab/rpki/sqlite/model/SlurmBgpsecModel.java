package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.SlurmBgpsec;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.SlurmBgpsecDbObject;

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
	private static final String GET_ALL = "getAll";
	private static final String GET_ALL_BY_TYPE = "getAllByType";
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
	 * Get all the {@link SlurmBgpsec}s, return empty list when no records are found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link SlurmBgpsec}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<SlurmBgpsec> getAll(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmBgpsecDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<SlurmBgpsec> slurmBgpsecs = new ArrayList<SlurmBgpsec>();
			while (rs.next()) {
				SlurmBgpsecDbObject slurmBgpsec = new SlurmBgpsecDbObject(rs);
				slurmBgpsecs.add(slurmBgpsec);
			}

			return slurmBgpsecs;
		}
	}

	/**
	 * Get all the {@link SlurmBgpsec}s by its type, return empty list when no
	 * records are found
	 * 
	 * @param type
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link SlurmBgpsec}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<SlurmBgpsec> getAllByType(int type, PagingParameters pagingParams, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE);
		query = Util.getQueryWithPaging(query, pagingParams, SlurmBgpsecDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setInt(1, type);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<SlurmBgpsec> slurmBgpsecs = new ArrayList<SlurmBgpsec>();
			while (rs.next()) {
				SlurmBgpsecDbObject slurmBgpsec = new SlurmBgpsecDbObject(rs);
				slurmBgpsecs.add(slurmBgpsec);
			}

			return slurmBgpsecs;
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
			statement.setInt(1, slurmBgpsec.getType());
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

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		SlurmBgpsecModel.queryGroup = queryGroup;
	}
}
