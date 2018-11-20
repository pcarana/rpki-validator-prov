package mx.nic.lab.rpki.sqlite.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.sqlite.SQLiteErrorCode;

import mx.nic.lab.rpki.sqlite.database.DatabaseSession;

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
			PreparedStatement result = null;
			while (true) {
				try {
					result = connection.prepareStatement(sql);
					result.setQueryTimeout(DatabaseSession.QUERY_TIMEOUT);
					break;
				} catch (SQLException e) {
					if (e.getErrorCode() != SQLiteErrorCode.SQLITE_BUSY.code) {
						throw e;
					}
					try {
						Thread.sleep(DatabaseSession.BUSY_RETRY_MS);
					} catch (InterruptedException ie) {
						// Keep trying
					}
				}
			}
			return result;
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
			ResultSet result = null;
			logger.log(Level.FINE, "Executing QUERY: " + statement.toString() + " from " + clazz.getName());
			while (true) {
				try {
					result = statement.executeQuery();
					break;
				} catch (SQLException e) {
					if (e.getErrorCode() != SQLiteErrorCode.SQLITE_BUSY.code) {
						throw e;
					}
					try {
						logger.log(Level.FINE,
								"Database is locked, sleeping " + DatabaseSession.BUSY_RETRY_MS + "ms to try again");
						Thread.sleep(DatabaseSession.BUSY_RETRY_MS);
					} catch (InterruptedException ie) {
						// Keep trying
					}
				}
			}
			return result;
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
			int result = -1;
			logger.log(Level.FINE, "Executing QUERY: " + statement.toString() + " from " + clazz.getName());
			while (true) {
				try {
					result = statement.executeUpdate();
					break;
				} catch (SQLException e) {
					if (e.getErrorCode() != SQLiteErrorCode.SQLITE_BUSY.code) {
						throw e;
					}
					try {
						logger.log(Level.FINE,
								"Database is locked, sleeping " + DatabaseSession.BUSY_RETRY_MS + "ms to try again");
						Thread.sleep(DatabaseSession.BUSY_RETRY_MS);
					} catch (InterruptedException ie) {
						// Keep trying
					}
				}
			}
			return result;
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
