package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.EncodedRpkiObject;

public class EncodedRpkiObjectDbObject extends EncodedRpkiObject implements DatabaseObject {

	public static final String ID_COLUMN = "ero_id";
	public static final String UPDATED_AT_COLUMN = "ero_updated_at";
	public static final String RPKI_OBJECT_COLUMN = "rpo_id";
	public static final String ENCODED_COLUMN = "ero_encoded";

	private Long rpkiObjectId;

	public EncodedRpkiObjectDbObject() {
		super();
	}

	public EncodedRpkiObjectDbObject(EncodedRpkiObject encodedRpkiObject) {
		this.setId(encodedRpkiObject.getId());
		this.setUpdatedAt(encodedRpkiObject.getUpdatedAt());
		this.setRpkiObject(encodedRpkiObject.getRpkiObject());
		this.setEncoded(encodedRpkiObject.getEncoded());
		if (getRpkiObject() != null) {
			this.setRpkiObjectId(getRpkiObject().getId());
		}
	}

	public EncodedRpkiObjectDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setRpkiObjectId(resultSet.getLong(RPKI_OBJECT_COLUMN));
		setUpdatedAt(getStringDateAsInstant(resultSet.getString(UPDATED_AT_COLUMN)));
		setEncoded(resultSet.getBytes(ENCODED_COLUMN));
		if (resultSet.wasNull()) {
			setEncoded(null);
		}
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		statement.setLong(1, getId());
		// updatedAt
		statement.setString(2, Instant.now().toString());
		if (getRpkiObjectId() != null) {
			statement.setLong(3, getRpkiObjectId());
		} else {
			statement.setNull(3, Types.NUMERIC);
		}
		if (getEncoded() != null) {
			statement.setBytes(4, getEncoded());
		} else {
			statement.setNull(4, Types.BLOB);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		List<ValidationError> validationErrors = new ArrayList<>();
		if (operation == Operation.CREATE) {

		}
		if (!validationErrors.isEmpty()) {
			throw new ValidationException(validationErrors);
		}
	}

	public Long getRpkiObjectId() {
		return rpkiObjectId;
	}

	public void setRpkiObjectId(Long rpkiObjectId) {
		this.rpkiObjectId = rpkiObjectId;
	}

}
