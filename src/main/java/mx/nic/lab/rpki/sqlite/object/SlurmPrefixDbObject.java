package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.pojo.SlurmPrefix;

/**
 * Extension of {@link SlurmPrefix} as a {@link DatabaseObject}
 *
 */
public class SlurmPrefixDbObject extends SlurmPrefix implements DatabaseObject {

	public static final String ID_COLUMN = "slp_id";
	public static final String ASN_COLUMN = "slp_asn";
	public static final String PREFIX_TEXT_COLUMN = "slp_prefix_text";
	public static final String START_PREFIX_COLUMN = "slp_start_prefix";
	public static final String END_PREFIX_COLUMN = "slp_end_prefix";
	public static final String PREFIX_LENGTH_COLUMN = "slp_prefix_length";
	public static final String PREFIX_MAX_LENGTH_COLUMN = "slp_prefix_max_length";
	public static final String TYPE_COLUMN = "slp_type";
	public static final String COMMENT_COLUMN = "slp_comment";

	public SlurmPrefixDbObject() {
		super();
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public SlurmPrefixDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setAsn(resultSet.getInt(ASN_COLUMN));
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
		setType(resultSet.getInt(TYPE_COLUMN));
		if (resultSet.wasNull()) {
			setType(null);
		}
		setComment(resultSet.getString(COMMENT_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		// FIXME This object can be stored to database, still pending to complete
	}
}
