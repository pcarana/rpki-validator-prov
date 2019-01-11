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
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.RoaDbObject;

/**
 * Model to retrieve ROA data from the database
 *
 */
public class RoaModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(RoaModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "Roa";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_BY_RPKI_OBJECT_ID = "getByRpkiObjectId";
	private static final String GET_ALL = "getAll";
	private static final String GET_ALL_COUNT = "getAllCount";
	private static final String FIND_EXACT_MATCH = "findExactMatch";
	private static final String FIND_COVERING_AGGREGATE = "findCoveringAggregate";
	private static final String FIND_MORE_SPECIFIC = "findMoreSpecific";
	private static final String EXIST_ASN = "existAsn";
	private static final String CREATE = "create";

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
	private static Class<RoaModel> getModelClass() {
		return RoaModel.class;
	}

	/**
	 * Get a {@link Roa} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link Roa} found
	 * @throws SQLException
	 */
	public static Roa getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, id);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RoaDbObject roa = null;
			do {
				roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
			} while (rs.next());

			return roa;
		}
	}

	/**
	 * Get all the {@link Roa}s found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The {@link ListResult} of {@link Roa}s found
	 * @throws SQLException
	 */
	public static ListResult<Roa> getAll(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, RoaDbObject.propertyToColumnMap);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			// Set the filter to the query (if the param was added)
			Util.setFilterParam(pagingParams, statement, 1);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
			}
			Integer totalFound = getAllCount(pagingParams, connection);
			return new ListResult<Roa>(roas, totalFound);
		}
	}

	/**
	 * Find a {@link Roa} that covers the specified prefix, return null if no record
	 * is found
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param connection
	 * @return The {@link Roa} that matches (or covers) the prefix
	 * @throws SQLException
	 */
	public static Roa findExactMatch(byte[] prefix, Integer prefixLength, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(FIND_EXACT_MATCH);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, prefix);
			statement.setInt(2, prefixLength);
			statement.setInt(3, prefixLength);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			RoaDbObject roa = null;
			do {
				roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
			} while (rs.next());

			return roa;
		}
	}

	/**
	 * Find all the candidate {@link Roa}s that are covering aggregates of the
	 * prefix received, this list still needs some work to effectively determine if
	 * any of the {@link Roa}s is a covering aggregate of the prefix (the database
	 * doesn't fully support binary operators on BINARYs, so thats why only the
	 * candidates are returned). Only the basic info of the ROAs is loaded.
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param familyType
	 * @param connection
	 * @return List of candidate {@link Roa}s that are covering aggregate of the
	 *         received prefix
	 * @throws SQLException
	 */
	public static List<Roa> findCoveringAggregate(byte[] prefix, Integer prefixLength, Integer familyType,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(FIND_COVERING_AGGREGATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, prefix);
			statement.setInt(2, prefixLength);
			statement.setInt(3, familyType);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				roas.add(new RoaDbObject(rs));
			}
			return roas;
		}
	}

	/**
	 * Find all the candidate {@link Roa}s that are more specific than the prefix
	 * received, this list still needs some work to effectively determine if any of
	 * the {@link Roa}s is more specific than the prefix (the database doesn't fully
	 * support binary operators on BINARYs, so thats why only the candidates are
	 * returned). Only the basic info of the ROAs is loaded.
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param familyType
	 * @param connection
	 * @return List of candidate {@link Roa}s that are more specific than the
	 *         received prefix
	 * @throws SQLException
	 */
	public static List<Roa> findMoreSpecific(byte[] prefix, Integer prefixLength, Integer familyType,
			Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(FIND_MORE_SPECIFIC);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, prefix);
			statement.setInt(2, prefixLength);
			statement.setInt(3, familyType);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				roas.add(new RoaDbObject(rs));
			}

			return roas;
		}
	}

	/**
	 * Check if there's at least one ROA with the specified ASN
	 * 
	 * @param asn
	 * @param connection
	 * @return <code>true</code> when there's at least one ROA with the ASN,
	 *         <code>false</code> otherwise
	 * @throws SQLException
	 */
	public static boolean existAsn(Long asn, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(EXIST_ASN);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, asn);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link Roa} returns <code>boolean</code> to indicate success
	 * 
	 * @param newRoa
	 * @param connection
	 * @return <code>boolean</code> to indicate success
	 * @throws SQLException
	 */
	public static boolean create(Roa newRoa, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			RoaDbObject stored = new RoaDbObject(newRoa);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
		}
	}

	/**
	 * Get all the {@link Roa}s related to a RPKI OBJECT ID, return empty list when
	 * no files are found
	 * 
	 * @param rpkiObjectId
	 * @param connection
	 * @return The list of {@link Roa}s related, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<Roa> getByRpkiObjectId(Long rpkiObjectId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_RPKI_OBJECT_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, rpkiObjectId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				RoaDbObject talFile = new RoaDbObject(rs);
				roas.add(talFile);
			}
			return roas;
		}
	}

	/**
	 * Load all the related objects to the ROA
	 * 
	 * @param roa
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(RoaDbObject roa, Connection connection) throws SQLException {
		roa.setRpkiObject(RpkiObjectModel.getById(roa.getRpkiObjectId(), connection));
		roa.setGbrs(GbrModel.getByRoa(roa, connection));
	}

	/**
	 * Get the count of all the {@link Roa}s, return 0 when no records are found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The count of all {@link Roa}s, or 0 when no data is found
	 * @throws SQLException
	 */
	private static Integer getAllCount(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_COUNT);
		query = Util.getQueryWithPaging(query, pagingParams, RoaDbObject.propertyToColumnMap);
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
		RoaModel.queryGroup = queryGroup;
	}
}
