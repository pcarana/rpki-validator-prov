package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.RoaDbObject;

/**
 * Model to retrieve ROA data from the database
 *
 */
public class RoaModel {

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
	private static final String GET_BY_UNIQUE = "getByUnique";
	private static final String GET_ALL = "getAll";
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
	 * Get a {@link Roa} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link Roa} found
	 * @throws SQLException
	 */
	public static Roa getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
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
	 * Get all the {@link Roa}s, return empty list when no records are found
	 * 
	 * @param pagingParams
	 * @param connection
	 * @return The list of {@link Roa}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<Roa> getAll(PagingParameters pagingParams, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		query = Util.getQueryWithPaging(query, pagingParams, RoaDbObject.propertyToColumnMap);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
			}
			return roas;
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setBytes(1, prefix);
			statement.setInt(2, prefixLength);
			statement.setInt(3, prefixLength);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
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
	 * any of the {@link Roa}s is a covering aggregate of the prefix (the SQLite
	 * database doesn't support binary operators on BLOBs, so thats why only the
	 * candidates are returned)
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param connection
	 * @return List of candidate {@link Roa}s that are covering aggregate of the
	 *         received prefix
	 * @throws SQLException
	 */
	public static List<Roa> findCoveringAggregate(byte[] prefix, Integer prefixLength, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(FIND_COVERING_AGGREGATE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setBytes(1, prefix);
			statement.setInt(2, prefixLength);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
			}

			return roas;
		}
	}

	/**
	 * Find all the candidate {@link Roa}s that are more specific than the prefix
	 * received, this list still needs some work to effectively determine if any of
	 * the {@link Roa}s is more specific than the prefix (the SQLite database
	 * doesn't support binary operators on BLOBs, so thats why only the candidates
	 * are returned)
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param connection
	 * @return List of candidate {@link Roa}s that are more specific than the
	 *         received prefix
	 * @throws SQLException
	 */
	public static List<Roa> findMoreSpecific(byte[] prefix, Integer prefixLength, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(FIND_MORE_SPECIFIC);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setBytes(1, prefix);
			statement.setInt(2, prefixLength);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			List<Roa> roas = new ArrayList<Roa>();
			while (rs.next()) {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, asn);
			ResultSet rs = statement.executeQuery();
			return rs.next();
		}
	}

	/**
	 * Creates a new {@link Roa} returns null if the object couldn't be created.
	 * 
	 * @param newRoa
	 * @param connection
	 * @return The ID of the {@link Roa} created
	 * @throws SQLException
	 */
	public static Long create(Roa newRoa, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			RoaDbObject stored = new RoaDbObject(newRoa);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			stored = getByUniqueFields(stored, connection);
			newRoa.setId(stored.getId());
			return newRoa.getId();
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
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, rpkiObjectId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
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
	 * Return a {@link PreparedStatement} that contains the necessary parameters to
	 * make a search of a unique {@link Roa} based on properties distinct that the
	 * ID
	 * 
	 * @param roa
	 * @param queryId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement prepareUniqueSearch(Roa roa, String queryId, Connection connection)
			throws SQLException {
		String query = getQueryGroup().getQuery(queryId);
		StringBuilder parameters = new StringBuilder();
		int currentIdx = 1;
		int rpoIdIdx = -1;
		int asnIdx = -1;
		int startPrefixIdx = -1;
		int endPrefixIdx = -1;
		int prefixLengthIdx = -1;
		int prefixMaxLengthIdx = -1;
		int prefixFamilyIdx = -1;
		if (roa.getRpkiObject() != null) {
			parameters.append(" and ").append(RoaDbObject.RPKI_OBJECT_COLUMN).append(" = ? ");
			rpoIdIdx = currentIdx++;
		}
		if (roa.getAsn() != null) {
			parameters.append(" and ").append(RoaDbObject.ASN_COLUMN).append(" = ? ");
			asnIdx = currentIdx++;
		}
		if (roa.getStartPrefix() != null) {
			parameters.append(" and ").append(RoaDbObject.START_PREFIX_COLUMN).append(" = ? ");
			startPrefixIdx = currentIdx++;
		}
		if (roa.getEndPrefix() != null) {
			parameters.append(" and ").append(RoaDbObject.END_PREFIX_COLUMN).append(" = ? ");
			endPrefixIdx = currentIdx++;
		}
		if (roa.getPrefixLength() != null) {
			parameters.append(" and ").append(RoaDbObject.PREFIX_LENGTH_COLUMN).append(" = ? ");
			prefixLengthIdx = currentIdx++;
		}
		if (roa.getPrefixMaxLength() != null) {
			parameters.append(" and ").append(RoaDbObject.PREFIX_MAX_LENGTH_COLUMN).append(" = ? ");
			prefixMaxLengthIdx = currentIdx++;
		}
		if (roa.getPrefixFamily() != null) {
			parameters.append(" and ").append(RoaDbObject.PREFIX_FAMILY_COLUMN).append(" = ? ");
			prefixFamilyIdx = currentIdx++;
		}
		query = query.replace("[and]", parameters.toString());
		PreparedStatement statement = connection.prepareStatement(query);
		if (rpoIdIdx > 0) {
			statement.setLong(rpoIdIdx, roa.getRpkiObject().getId());
		}
		if (asnIdx > 0) {
			statement.setLong(asnIdx, roa.getAsn());
		}
		if (startPrefixIdx > 0) {
			statement.setBytes(startPrefixIdx, roa.getStartPrefix());
		}
		if (endPrefixIdx > 0) {
			statement.setBytes(endPrefixIdx, roa.getEndPrefix());
		}
		if (prefixLengthIdx > 0) {
			statement.setInt(prefixLengthIdx, roa.getPrefixLength());
		}
		if (prefixMaxLengthIdx > 0) {
			statement.setInt(prefixMaxLengthIdx, roa.getPrefixMaxLength());
		}
		if (prefixFamilyIdx > 0) {
			statement.setInt(prefixFamilyIdx, roa.getPrefixFamily());
		}
		return statement;
	}

	/**
	 * Get a {@link Roa} by its unique fields
	 * 
	 * @param roa
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static RoaDbObject getByUniqueFields(Roa roa, Connection connection) throws SQLException {
		try (PreparedStatement statement = prepareUniqueSearch(roa, GET_BY_UNIQUE, connection)) {
			ResultSet resultSet = statement.executeQuery();
			RoaDbObject found = new RoaDbObject(resultSet);
			loadRelatedObjects(found, connection);
			return found;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RoaModel.queryGroup = queryGroup;
	}
}
