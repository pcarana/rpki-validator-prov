package mx.nic.lab.rpki.sqlite.object;

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
	public static final String LAST_SYNC_COLUMN = "tal_last_sync";
	public static final String PUBLIC_KEY_COLUMN = "tal_public_key";
	public static final String SYNC_STATUS_COLUMN = "tal_sync_status";
	public static final String VALIDATION_STATUS_COLUMN = "tal_validation_status";
	public static final String NAME_COLUMN = "tal_name";
	public static final String LOADED_CER_COLUMN = "tal_loaded_cer";

	/**
	 * Mapping of the {@link Tal} properties to its corresponding DB column
	 */
	public static final Map<String, String> propertyToColumnMap;
	static {
		propertyToColumnMap = new HashMap<>();
		propertyToColumnMap.put(ID, ID_COLUMN);
		propertyToColumnMap.put(LAST_SYNC, LAST_SYNC_COLUMN);
		propertyToColumnMap.put(PUBLIC_KEY, PUBLIC_KEY_COLUMN);
		propertyToColumnMap.put(SYNC_STATUS, SYNC_STATUS_COLUMN);
		propertyToColumnMap.put(VALIDATION_STATUS, VALIDATION_STATUS_COLUMN);
		propertyToColumnMap.put(NAME, NAME_COLUMN);
	}

	public TalDbObject() {
		super();
	}

	public TalDbObject(Tal tal) {
		this.setId(tal.getId());
		this.setLastSync(tal.getLastSync());
		this.setPublicKey(tal.getPublicKey());
		this.setSyncStatus(tal.getSyncStatus());
		this.setValidationStatus(tal.getValidationStatus());
		this.setName(tal.getName());
		this.setLoadedCer(tal.getLoadedCer());
		this.setTalUris(new ArrayList<>(tal.getTalUris()));
		this.setTalFiles(new ArrayList<>(tal.getTalFiles()));
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
		setLastSync(resultSet.getString(LAST_SYNC_COLUMN));
		setPublicKey(resultSet.getString(PUBLIC_KEY_COLUMN));
		setSyncStatus(resultSet.getString(SYNC_STATUS_COLUMN));
		setValidationStatus(resultSet.getString(VALIDATION_STATUS_COLUMN));
		setName(resultSet.getString(NAME_COLUMN));
		setLoadedCer(resultSet.getBytes(LOADED_CER_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		statement.setLong(1, getId());
		if (getLastSync() != null) {
			statement.setString(2, getLastSync());
		} else {
			statement.setNull(2, Types.VARCHAR);
		}
		if (getPublicKey() != null) {
			statement.setString(3, getPublicKey());
		} else {
			statement.setNull(3, Types.VARCHAR);
		}
		if (getSyncStatus() != null) {
			statement.setString(4, getSyncStatus());
		} else {
			statement.setNull(4, Types.VARCHAR);
		}
		if (getValidationStatus() != null) {
			statement.setString(5, getValidationStatus());
		} else {
			statement.setNull(5, Types.VARCHAR);
		}
		if (getName() != null) {
			statement.setString(6, getName());
		} else {
			statement.setNull(6, Types.VARCHAR);
		}
		if (getLoadedCer() != null) {
			statement.setBytes(7, getLoadedCer());
		} else {
			statement.setNull(7, Types.BLOB);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		List<ValidationError> validationErrors = new ArrayList<>();
		if (operation == Operation.CREATE) {
			String publicKey = this.getPublicKey();
			String syncStatus = this.getSyncStatus();
			String validationStatus = this.getValidationStatus();
			String name = this.getName();
			if (publicKey == null || publicKey.trim().isEmpty()) {
				validationErrors.add(new ValidationError(OBJECT_NAME, PUBLIC_KEY, null, ValidationErrorType.NULL));
			}
			if (syncStatus == null || syncStatus.trim().isEmpty()) {
				validationErrors.add(new ValidationError(OBJECT_NAME, SYNC_STATUS, null, ValidationErrorType.NULL));
			} else {
				try {
					Enum.valueOf(Tal.SyncStatus.class, syncStatus);
				} catch (IllegalArgumentException e) {
					validationErrors.add(new ValidationError(OBJECT_NAME, SYNC_STATUS, syncStatus,
							ValidationErrorType.UNEXPECTED_VALUE));
				}
			}
			if (validationStatus == null || validationStatus.trim().isEmpty()) {
				validationErrors
						.add(new ValidationError(OBJECT_NAME, VALIDATION_STATUS, null, ValidationErrorType.NULL));
			} else {
				try {
					Enum.valueOf(Tal.ValidationStatus.class, validationStatus);
				} catch (IllegalArgumentException e) {
					validationErrors.add(new ValidationError(OBJECT_NAME, VALIDATION_STATUS, validationStatus,
							ValidationErrorType.UNEXPECTED_VALUE));
				}
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
