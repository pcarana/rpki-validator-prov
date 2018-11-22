package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.Slurm;
import mx.nic.lab.rpki.db.pojo.SlurmBgpsec;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;

/**
 * Model to retrieve SLURM complete data from the database (prefixes and
 * BGPsec). This class uses the implementations of {@link SlurmPrefixModel} and
 * {@link SlurmBgpsecModel}.
 *
 */
public class SlurmModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(SlurmModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "Slurm";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_LAST_CHECKSUM = "getLastChecksum";
	private static final String UPDATE_LAST_CHECKSUM = "updateLastChecksum";

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
	private static Class<SlurmModel> getModelClass() {
		return SlurmModel.class;
	}

	/**
	 * Get the complete {@link Slurm}, with both {@link SlurmPrefix} and
	 * {@link SlurmBgpsec} lists.
	 * 
	 * @param connection
	 * @return The complete SLURM, if no rules are found then the object returned
	 *         has empty lists
	 * @throws SQLException
	 */
	public static Slurm getAll(Connection connection) throws SQLException {
		List<SlurmPrefix> prefixes = SlurmPrefixModel.getAll(null, connection).getResults();
		List<SlurmBgpsec> bgpsecs = SlurmBgpsecModel.getAll(null, connection).getResults();
		Slurm slurm = new Slurm();
		prefixes.forEach((prefix) -> {
			if (prefix.getType().equals(SlurmPrefix.TYPE_ASSERTION)) {
				slurm.getLocallyAddedAssertions().getPrefixes().add(prefix);
			} else {
				slurm.getValidationOutputFilters().getPrefixes().add(prefix);
			}
		});
		bgpsecs.forEach((bgpsec) -> {
			if (bgpsec.getType().equals(SlurmBgpsec.TYPE_ASSERTION)) {
				slurm.getLocallyAddedAssertions().getBgpsecs().add(bgpsec);
			} else {
				slurm.getValidationOutputFilters().getBgpsecs().add(bgpsec);
			}
		});
		return slurm;
	}

	/**
	 * Get the last checksum of the SLURM file that was synchronized
	 * 
	 * @param connection
	 * @return SLURM checksum or null if there's no checksum yet
	 * @throws SQLException
	 */
	public static byte[] getLastChecksum(Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_LAST_CHECKSUM);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (!rs.next()) {
				return null;
			}
			return rs.getBytes("sch_checksum");
		}
	}

	/**
	 * Update the last checksum of the SLURM file
	 * 
	 * @param newChecksum
	 *            New checksum to assign
	 * @param connection
	 * @return Number of affected rows
	 * @throws SQLException
	 */
	public static int updateLastChecksum(byte[] newChecksum, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(UPDATE_LAST_CHECKSUM);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, newChecksum);
			return executeUpdate(statement, getModelClass(), logger);
		}
	}

	/**
	 * Delete the whole SLURM (filters and assertions)
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	public static int deleteAll(Connection connection) throws SQLException {
		int totalDeleted = 0;
		totalDeleted += SlurmPrefixModel.deleteAll(connection);
		totalDeleted += SlurmBgpsecModel.deleteAll(connection);
		return totalDeleted;
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		SlurmModel.queryGroup = queryGroup;
	}
}
