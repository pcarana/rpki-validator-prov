package mx.nic.lab.rpki.prov.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.Tal;

public class TalDbObject extends Tal implements DatabaseObject {

	public static final String ID_COLUMN = "tal_id";
	public static final String PUBLIC_KEY_COLUMN = "tal_public_key";
	public static final String NAME_COLUMN = "tal_name";
	public static final String LOADED_CER_COLUMN = "tal_loaded_cer";

	/**
	 * Mapping of the {@link Tal} properties to its corresponding DB column
	 */
	public static final Map<String, String> propertyToColumnMap;
	static {
		propertyToColumnMap = new HashMap<>();
		propertyToColumnMap.put(ID, ID_COLUMN);
		propertyToColumnMap.put(PUBLIC_KEY, PUBLIC_KEY_COLUMN);
		propertyToColumnMap.put(NAME, NAME_COLUMN);
	}

	public TalDbObject() {
		super();
	}

	public TalDbObject(Tal tal) {
		this.setId(tal.getId());
		this.setPublicKey(tal.getPublicKey());
		this.setName(tal.getName());
		this.setLoadedCer(tal.getLoadedCer());
		this.setTalUris(new ArrayList<>(tal.getTalUris()));
		this.setValidationRuns(new ArrayList<>(tal.getValidationRuns()));
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public TalDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setPublicKey(resultSet.getString(PUBLIC_KEY_COLUMN));
		setName(resultSet.getString(NAME_COLUMN));
		setLoadedCer(resultSet.getBytes(LOADED_CER_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		if (getPublicKey() != null) {
			statement.setString(1, getPublicKey());
		} else {
			statement.setNull(1, Types.VARCHAR);
		}
		if (getName() != null) {
			statement.setString(2, getName());
		} else {
			statement.setNull(2, Types.VARCHAR);
		}
		if (getLoadedCer() != null) {
			statement.setBytes(3, getLoadedCer());
		} else {
			statement.setNull(3, Types.BLOB);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		List<ValidationError> validationErrors = new ArrayList<>();
		if (operation == Operation.CREATE) {
			String publicKey = this.getPublicKey();
			String name = this.getName();
			if (publicKey == null || publicKey.trim().isEmpty()) {
				validationErrors.add(new ValidationError(OBJECT_NAME, PUBLIC_KEY, null, ValidationErrorType.NULL));
			}
			if (name == null || name.trim().isEmpty()) {
				validationErrors.add(new ValidationError(OBJECT_NAME, NAME, null, ValidationErrorType.NULL));
			}
		}
		if (!validationErrors.isEmpty()) {
			throw new ValidationException(validationErrors);
		}
	}
}
