package mx.nic.lab.rpki.prov.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.TalUri;

/**
 * Extension of {@link TalUri} as a {@link DatabaseObject}
 *
 */
public class TalUriDbObject extends TalUri implements DatabaseObject {

	public static final String ID_COLUMN = "tau_id";
	public static final String TAL_ID_COLUMN = "tal_id";
	public static final String LOCATION_COLUMN = "tau_location";

	public TalUriDbObject() {
		super();
	}

	public TalUriDbObject(TalUri talUri) {
		this.setId(talUri.getId());
		this.setTalId(talUri.getTalId());
		this.setLocation(talUri.getLocation());
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public TalUriDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setTalId(resultSet.getLong(TAL_ID_COLUMN));
		if (resultSet.wasNull()) {
			setTalId(null);
		}
		setLocation(resultSet.getString(LOCATION_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		statement.setLong(1, getId());
		statement.setLong(2, getTalId());
		if (getLocation() != null) {
			statement.setString(3, getLocation());
		} else {
			statement.setNull(3, Types.VARCHAR);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		// No special validations for now
	}
}
