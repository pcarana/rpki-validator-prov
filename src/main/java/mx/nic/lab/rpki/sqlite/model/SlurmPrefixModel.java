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

import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.SlurmPrefixDbObject;

/**
 * Model to retrieve SLURM Prefix data from the database
 *
 */
public class SlurmPrefixModel {

	private static final Logger logger = Logger.getLogger(SlurmPrefixModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "SlurmPrefix";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_ID = "getById";
	private static final String GET_ALL = "getAll";
	private static final String GET_ALL_BY_TYPE = "getAllByType";

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
	 * Get a {@link SlurmPrefix} by its ID, return null if no data is found
	 * 
	 * @param id
	 * @param connection
	 * @return The {@link SlurmPrefix} found
	 * @throws SQLException
	 */
	public static SlurmPrefix getById(Long id, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_BY_ID);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setLong(1, id);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			SlurmPrefix slurmPrefix = null;
			do {
				slurmPrefix = new SlurmPrefixDbObject(rs);
			} while (rs.next());

			return slurmPrefix;
		}
	}

	/**
	 * Get all the {@link SlurmPrefix}s, return empty list when no records are found
	 * 
	 * @param connection
	 * @return The list of {@link SlurmPrefix}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<SlurmPrefix> getAll(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<SlurmPrefix> slurmPrefixes = new ArrayList<SlurmPrefix>();
			do {
				SlurmPrefixDbObject slurmPrefix = new SlurmPrefixDbObject(rs);
				slurmPrefixes.add(slurmPrefix);
			} while (rs.next());

			return slurmPrefixes;
		}
	}

	/**
	 * Get all the {@link SlurmPrefix}s by its type, return empty list when no
	 * records are found
	 * 
	 * @param connection
	 * @param id
	 * @return The list of {@link SlurmPrefix}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<SlurmPrefix> getAllByType(Connection connection, int type) throws SQLException {
		String query = getQueryGroup().getQuery(GET_ALL_BY_TYPE);
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setInt(1, type);
			logger.log(Level.INFO, "Executing QUERY: " + statement.toString());
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return Collections.emptyList();
			}
			List<SlurmPrefix> slurmPrefixes = new ArrayList<SlurmPrefix>();
			do {
				SlurmPrefixDbObject slurmPrefix = new SlurmPrefixDbObject(rs);
				slurmPrefixes.add(slurmPrefix);
			} while (rs.next());

			return slurmPrefixes;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		SlurmPrefixModel.queryGroup = queryGroup;
	}
}
