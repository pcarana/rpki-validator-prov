package mx.nic.lab.rpki.prov.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import mx.nic.lab.rpki.prov.database.DatabaseSession;

/**
 * Main class that SHOULD be used to execute all the queries at DB in a
 * synchronous way
 *
 */
public class DatabaseModel {

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
			PreparedStatement result = connection.prepareStatement(sql);
			result.setQueryTimeout(DatabaseSession.QUERY_TIMEOUT);
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
			logger.log(Level.FINE, "Executing QUERY: " + statement.toString() + " from " + clazz.getName());
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
			logger.log(Level.FINE, "Executing QUERY: " + statement.toString() + " from " + clazz.getName());
			return statement.executeUpdate();
		}
	}
}
