package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.RtrSession;

/**
 * Extension of {@link RtrSession} as a {@link DatabaseObject}
 *
 */
public class RtrSessionDbObject extends RtrSession implements DatabaseObject {

	public static final String ID_COLUMN = "rts_id";
	public static final String ADDRESS_COLUMN = "rts_address";
	public static final String PORT_COLUMN = "rts_port";
	public static final String STATUS_COLUMN = "rts_status";
	public static final String LAST_REQUEST_COLUMN = "rts_last_request";
	public static final String LAST_RESPONSE_COLUMN = "rts_last_response";
	public static final String SESSION_ID_COLUMN = "rts_session_id";
	public static final String SERIAL_NUMBER_COLUMN = "rts_serial_number";
	public static final String VERSION_COLUMN = "rts_version";

	public RtrSessionDbObject() {
		super();
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public RtrSessionDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setAddress(resultSet.getString(ADDRESS_COLUMN));
		setPort(resultSet.getLong(PORT_COLUMN));
		if (resultSet.wasNull()) {
			setPort(null);
		}
		setStatus(resultSet.getString(STATUS_COLUMN));
		setLastRequest(resultSet.getString(LAST_REQUEST_COLUMN));
		setLastResponse(resultSet.getString(LAST_RESPONSE_COLUMN));
		setSessionId(resultSet.getLong(SESSION_ID_COLUMN));
		if (resultSet.wasNull()) {
			setSessionId(null);
		}
		setSerialNumber(resultSet.getLong(SERIAL_NUMBER_COLUMN));
		if (resultSet.wasNull()) {
			setSerialNumber(null);
		}
		setVersion(resultSet.getInt(VERSION_COLUMN));
		if (resultSet.wasNull()) {
			setVersion(null);
		}
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		// This object can't be stored to database
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		// No special validations for now
	}
}
