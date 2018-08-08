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

import mx.nic.lab.rpki.db.pojo.RtrSession;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.RtrSessionDbObject;

/**
 * Model to retrieve RTR sessions data from the database
 *
 */
public class RtrSessionModel {

	private static final Logger logger = Logger.getLogger(RtrSessionModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "RtrSession";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
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
	 * Get all the {@link RtrSession}s, return empty list when no files are found
	 * 
	 * @param connection
	 * @return The list of {@link RtrSession}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<RtrSession> getAll(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<RtrSession> rtrSessions = new ArrayList<RtrSession>();
			do {
				RtrSessionDbObject rtrSession = new RtrSessionDbObject(rs);
				rtrSessions.add(rtrSession);
			} while (rs.next());

			return rtrSessions;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		RtrSessionModel.queryGroup = queryGroup;
	}
}
