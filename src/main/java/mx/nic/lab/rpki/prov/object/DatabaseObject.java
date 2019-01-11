package mx.nic.lab.rpki.prov.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import mx.nic.lab.rpki.db.exception.ValidationException;

/**
 * An object that can be loaded and stored in Database (if required)
 *
 */
public interface DatabaseObject {

	/**
	 * Format used by SQLite to represent dates as TEXT fields (ISO8601, check
	 * <a href="https://www.sqlite.org/datatype3.html#date_and_time_datatype">SQLite
	 * Date and Time Datatype</a>)
	 */
	public static final String DATE_AS_TEXT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

	/**
	 * Possible operations to perform over an instance of a {@link DatabaseObject}
	 */
	public enum Operation {
		CREATE, UPDATE, DELETE
	}

	/**
	 * Get a string loaded from the DB and return it as {@link Instant}, return null
	 * if there was an error
	 * 
	 * @param stringDate
	 * @return
	 */
	public static Instant getStringDateAsInstant(String stringDate) {
		if (stringDate == null) {
			return null;
		}
		SimpleDateFormat df = new SimpleDateFormat(DATE_AS_TEXT_FORMAT);
		try {
			Date tempDate = df.parse(stringDate);
			return tempDate.toInstant();
		} catch (ParseException e) {
			return null;
		}
	}

	/**
	 * Get a string from the DB and return it as an {@link Enum} of type
	 * <code>T</code>, return null if there was an error
	 * 
	 * @param enumClass
	 * @param value
	 * @return
	 */
	public static <T extends Enum<T>> T getStringAsEnum(Class<T> enumClass, String value) {
		if (value == null) {
			return null;
		}
		try {
			return Enum.valueOf(enumClass, value);
		} catch (IllegalArgumentException e) {
			return null;
		}
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
	 * @throws ValidationException
	 *             if something goes wrong
	 */
	public void validate(Operation operation) throws ValidationException;
}
