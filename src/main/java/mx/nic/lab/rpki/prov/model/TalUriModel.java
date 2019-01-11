package mx.nic.lab.rpki.prov.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.TalUri;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.TalUriDbObject;

/**
 * Model to retrieve TAL URIs data from the database
 *
 */
public class TalUriModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(TalUriModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "TalUri";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_TAL_ID = "getByTalId";
	private static final String GET_LAST_ID = "getLastId";
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
	private static Class<TalUriModel> getModelClass() {
		return TalUriModel.class;
	}

	/**
	 * Get all the {@link TalUri}s related to a TAL ID, return empty list when no
	 * files are found
	 * 
	 * @param talId
	 * @param connection
	 * @return The list of {@link TalUri}s related, or empty list when no data is
	 *         found
	 * @throws SQLException
	 */
	public static List<TalUri> getByTalId(Long talId, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_TAL_ID);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setLong(1, talId);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			List<TalUri> talUris = new ArrayList<TalUri>();
			while (rs.next()) {
				TalUriDbObject talUri = new TalUriDbObject(rs);
				talUris.add(talUri);
			}

			return talUris;
		}
	}

	/**
	 * Creates a new {@link TalUri} returns null if the object couldn't be created.
	 * 
	 * @param newTalUri
	 * @param connection
	 * @return The ID of the {@link TalUri} created
	 * @throws SQLException
	 */
	public static Long create(TalUri newTalUri, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			Long newId = getLastId(connection) + 1;
			newTalUri.setId(newId);
			TalUriDbObject stored = new TalUriDbObject(newTalUri);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			if (created < 1) {
				return null;
			}
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
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			// First in the table
			if (!rs.next()) {
				return 0L;
			}
			return rs.getLong(1);
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		TalUriModel.queryGroup = queryGroup;
	}
}
