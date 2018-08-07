package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.Gbr;

public class GbrDbObject extends Gbr implements DatabaseObject {

	public static final String ID_COLUMN = "gbr_id";
	public static final String VCARD_COLUMN = "gbr_vcard";
	public static final String CMS_DATA_COLUMN = "gbr_cms_data";

	public GbrDbObject() {
		super();
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public GbrDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setVcard(resultSet.getString(VCARD_COLUMN));
		setCmsData(resultSet.getBytes(CMS_DATA_COLUMN));
		if (resultSet.wasNull()) {
			setCmsData(null);
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
