package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.SlurmBgpsec;

/**
 * Extension of {@link SlurmBgpsec} as a {@link DatabaseObject}
 *
 */
public class SlurmBgpsecDbObject extends SlurmBgpsec implements DatabaseObject {

	public static final String ID_COLUMN = "slb_id";
	public static final String ASN_COLUMN = "slb_asn";
	public static final String SKI_COLUMN = "slb_ski";
	public static final String PUBLIC_KEY_COLUMN = "slb_public_key";
	public static final String TYPE_COLUMN = "slb_type";
	public static final String COMMENT_COLUMN = "slb_comment";

	public SlurmBgpsecDbObject() {
		super();
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public SlurmBgpsecDbObject(ResultSet resultSet) throws SQLException {
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
		setSki(resultSet.getString(SKI_COLUMN));
		setPublicKey(resultSet.getString(PUBLIC_KEY_COLUMN));
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

	@Override
	public void validate(Operation operation) throws ValidationException {
		// FIXME Add validations
	}
}
