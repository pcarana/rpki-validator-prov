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
	private static final String GET_ALL = "getAll";
	private static final String FIND_EXACT_MATCH = "findExactMatch";
	private static final String FIND_COVERING_AGGREGATE = "findCoveringAggregate";
	private static final String FIND_MORE_SPECIFIC = "findMoreSpecific";
	private static final String EXIST_ASN = "existAsn";

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
			Roa roa = null;
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
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<Roa> roas = new ArrayList<Roa>();
			do {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
			} while (rs.next());

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
			Roa roa = null;
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
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<Roa> roas = new ArrayList<Roa>();
			do {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
			} while (rs.next());

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
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<Roa> roas = new ArrayList<Roa>();
			do {
				RoaDbObject roa = new RoaDbObject(rs);
				loadRelatedObjects(roa, connection);
				roas.add(roa);
			} while (rs.next());

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
	 * Load all the related objects to the ROA
	 * 
	 * @param roa
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadRelatedObjects(Roa roa, Connection connection) throws SQLException {
		Long roaId = roa.getId();
		roa.setGbrs(GbrModel.getByRoaId(roaId, connection));
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RoaModel.queryGroup = queryGroup;
	}
}
