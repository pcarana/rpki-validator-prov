package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.Gbr;

public class GbrDbObject extends Gbr implements DatabaseObject {

	public static final String RPKI_OBJECT_COLUMN = "rpo_id";
	public static final String ID_COLUMN = "gbr_id";
	public static final String VCARD_COLUMN = "gbr_vcard";

	private Long rpkiObjectId;

	public GbrDbObject() {
		super();
	}

	public GbrDbObject(Gbr gbr) {
		this.setId(gbr.getId());
		this.setRpkiObject(gbr.getRpkiObject());
		this.setVcard(gbr.getVcard());
		if (getRpkiObject() != null) {
			this.setRpkiObjectId(getRpkiObject().getId());
		}
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
		setRpkiObjectId(resultSet.getLong(RPKI_OBJECT_COLUMN));
		setId(resultSet.getLong(ID_COLUMN));
		setVcard(resultSet.getString(VCARD_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		// TODO This object should be stored to database
		// updatedAt
		// statement.setString(2, Instant.now().toString());
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		// No special validations for now
	}

	public Long getRpkiObjectId() {
		return rpkiObjectId;
	}

	public void setRpkiObjectId(Long rpkiObjectId) {
		this.rpkiObjectId = rpkiObjectId;
	}
}
