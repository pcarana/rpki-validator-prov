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
	 * @param connection
	 * @return The list of {@link Roa}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<Roa> getAll(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
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
