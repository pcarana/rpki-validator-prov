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

import mx.nic.lab.rpki.db.pojo.Gbr;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.GbrDbObject;

/**
 * Model to retrieve GBR data from the database
 *
 */
public class GbrModel {

	private final static Logger logger = Logger.getLogger(GbrModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private final static String QUERY_GROUP = "Gbr";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ROA_ID = "getByRoaId";

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
	 * Get all the {@link Gbr}s related to a {@link Roa} by its ID, returns an empty
	 * list when no data is found
	 * 
	 * @param roaId
	 * @param connection
	 * @return The list of {@link Gbr}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<Gbr> getByRoaId(Long roaId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ROA_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, roaId);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<Gbr> gbrs = new ArrayList<Gbr>();
			do {
				GbrDbObject gbr = new GbrDbObject(rs);
				gbrs.add(gbr);
			} while (rs.next());

			return gbrs;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		GbrModel.queryGroup = queryGroup;
	}
}
