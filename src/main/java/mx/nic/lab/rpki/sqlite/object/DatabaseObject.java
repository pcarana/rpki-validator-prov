package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;

/**
 * An object that can be loaded and stored in Database (if required)
 *
 */
public interface DatabaseObject {

	/**
	 * Possible operations to perform over an instance of a {@link DatabaseObject}
	 */
	public enum Operation {
		CREATE, UPDATE, DELETE
	}

	/**
	 * Load the object information from the <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public void loadFromDatabase(ResultSet resultSet) throws SQLException;

	/**
	 * Set the <code>PreparedStatement</code> so that the Object instance can be
	 * stored to database
	 * 
	 * @param statement
	 * @throws SQLException
	 */
	public void storeToDatabase(PreparedStatement statement) throws SQLException;

	/**
	 * Validates the object so that the {@link Operation} can be performed
	 * 
	 * @param operation
	 *            {@link Operation} that will be performed on the object instance
	 * @throws ApiDataAccessException
	 *             if something goes wrong
	 */
	public void validate(Operation operation) throws ApiDataAccessException;
}
