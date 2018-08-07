package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.Roa;

/**
 * Extension of {@link Roa} as a {@link DatabaseObject}
 *
 */
public class RoaDbObject extends Roa implements DatabaseObject {

	public static final String ID_COLUMN = "roa_id";
	public static final String ASN_COLUMN = "roa_asn";
	public static final String PREFIX_TEXT_COLUMN = "roa_prefix_text";
	public static final String START_PREFIX_COLUMN = "roa_start_prefix";
	public static final String END_PREFIX_COLUMN = "roa_end_prefix";
	public static final String PREFIX_LENGTH_COLUMN = "roa_prefix_length";
	public static final String PREFIX_MAX_LENGTH_COLUMN = "roa_prefix_max_length";
	public static final String CMS_DATA_COLUMN = "roa_cms_data";
	public static final String TAL_ID_COLUMN = "tal_id";

	public RoaDbObject() {
		super();
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public RoaDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setAsn(resultSet.getLong(ASN_COLUMN));
		if (resultSet.wasNull()) {
			setAsn(null);
		}
		setPrefixText(resultSet.getString(PREFIX_TEXT_COLUMN));
		setStartPrefix(resultSet.getBytes(START_PREFIX_COLUMN));
		if (resultSet.wasNull()) {
			setStartPrefix(null);
		}
		setEndPrefix(resultSet.getBytes(END_PREFIX_COLUMN));
		if (resultSet.wasNull()) {
			setEndPrefix(null);
		}
		setPrefixLength(resultSet.getInt(PREFIX_LENGTH_COLUMN));
		if (resultSet.wasNull()) {
			setPrefixLength(null);
		}
		setPrefixMaxLength(resultSet.getInt(PREFIX_MAX_LENGTH_COLUMN));
		if (resultSet.wasNull()) {
			setPrefixMaxLength(null);
		}
		setCmsData(resultSet.getBytes(CMS_DATA_COLUMN));
		if (resultSet.wasNull()) {
			setCmsData(null);
		}
		setTalId(resultSet.getLong(TAL_ID_COLUMN));
		if (resultSet.wasNull()) {
			setTalId(null);
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
