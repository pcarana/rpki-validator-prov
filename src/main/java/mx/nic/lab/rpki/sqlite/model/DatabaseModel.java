package mx.nic.lab.rpki.sqlite.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class that SHOULD be used to execute all the queries at DB in a
 * synchronous way; this is done to avoid an SQLITE_BUSY error when the database
 * is locked
 *
 */
public class DatabaseModel {

	/**
	 * Main query to get the last inserted ID
	 */
	private static final String GET_LAST_ROWID_SQL = "select last_insert_rowid()";

	/**
	 * Get a {@link PreparedStatement} from the <code>connection</code> in a
	 * synchronized manner, the <code>clazz</code> sent is used as a lock
	 * 
	 * @param connection
	 *            DB connection
	 * @param sql
	 *            SQL used for the {@link PreparedStatement}
	 * @param clazz
	 *            {@link Class} to use as lock
	 * @return
	 * @throws SQLException
	 */
	public static <T> PreparedStatement prepareStatement(Connection connection, String sql, Class<T> clazz)
			throws SQLException {
		synchronized (clazz) {
			return connection.prepareStatement(sql);
		}
	}

	/**
	 * Return the {@link ResultSet} of the {@link PreparedStatement#executeQuery()}
	 * using the <code>statement</code> sent and the <code>clazz</code> a lock
	 * 
	 * @param statement
	 * @param clazz
	 * @return
	 * @throws SQLException
	 */
	public static <T> ResultSet executeQuery(PreparedStatement statement, Class<T> clazz, Logger logger)
			throws SQLException {
		synchronized (clazz) {
			logger.log(Level.FINE, "Executing QUERY: " + statement.toString());
			return statement.executeQuery();
		}
	}

	/**
	 * Return the affected rows result of the
	 * {@link PreparedStatement#executeUpdate()} using the <code>statement</code>
	 * sent and the <code>clazz</code> a lock
	 * 
	 * @param statement
	 * @param clazz
	 * @return
	 * @throws SQLException
	 */
	public static <T> int executeUpdate(PreparedStatement statement, Class<T> clazz, Logger logger)
			throws SQLException {
		synchronized (clazz) {
			logger.log(Level.FINE, "Executing QUERY: " + statement.toString());
			return statement.executeUpdate();
		}
	}

	/**
	 * Get the last rowid used in an insert statement, using the SQLite
	 * 'last_insert_rowid()' function
	 * 
	 * @param connection
	 * @param clazz
	 * @return
	 * @throws SQLException
	 */
	public static <T> Long getLastRowid(Connection connection, Class<T> clazz, Logger logger) throws SQLException {
		try (PreparedStatement statement = prepareStatement(connection, GET_LAST_ROWID_SQL, clazz)) {
			ResultSet resultSet = executeQuery(statement, clazz, logger);
			return resultSet.getLong(1);
		}
	}
}
