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

import mx.nic.lab.rpki.db.pojo.Gbr;
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
	private static final String GET_ALL = "getAll";
	private static final String FIND_EXACT_MATCH = "findExactMatch";
	private static final String FIND_COVERING_AGGREGATE = "findCoveringAggregate";
	private static final String FIND_MORE_SPECIFIC = "findMoreSpecific";
	private static final String EXIST_ASN = "existAsn";
	private static final String GET_LAST_ID = "getLastId";
	private static final String CREATE = "create";
	private static final String CREATE_GBR_RELATION = "createGbrRelation";

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
			Long newId = getLastId(connection) + 1;
			newRoa.setId(newId);
			RoaDbObject stored = new RoaDbObject(newRoa);
			stored.storeToDatabase(statement);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			if (created < 1) {
				return null;
			}
			storeRelatedObjects(newRoa, connection);
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
		Long roaId = roa.getId();
		roa.setGbrs(GbrModel.getByRoaId(roaId, connection));
	}

	/**
	 * Store the related objects to the ROA
	 * 
	 * @param roa
	 * @param connection
	 * @throws SQLException
	 */
	private static void storeRelatedObjects(Roa roa, Connection connection) throws SQLException {
		for (Gbr gbr : roa.getGbrs()) {
			// TODO This assumes that the GBRs already exists, is this correct?
			createGbrRelation(roa.getId(), gbr.getId(), connection);
		}
	}

	/**
	 * Creates a {@link Roa} relation with a {@link Gbr} based on the IDs of both of
	 * them
	 * 
	 * @param roaId
	 * @param gbrId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static boolean createGbrRelation(Long roaId, Long gbrId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE_GBR_RELATION);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, roaId);
			statement.setLong(2, gbrId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			int created = statement.executeUpdate();
			return created > 0;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RoaModel.queryGroup = queryGroup;
	}
}
