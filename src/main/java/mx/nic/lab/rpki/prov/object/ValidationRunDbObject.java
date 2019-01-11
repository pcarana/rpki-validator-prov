package mx.nic.lab.rpki.prov.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.ValidationRun;

public class ValidationRunDbObject extends ValidationRun implements DatabaseObject {

	public static final String ID_COLUMN = "var_id";
	public static final String UPDATED_AT_COLUMN = "var_updated_at";
	public static final String COMPLETED_AT_COLUMN = "var_completed_at";
	public static final String STATUS_COLUMN = "var_status";
	public static final String TYPE_COLUMN = "var_type";
	public static final String TAL_ID_COLUMN = "tal_id";
	public static final String TAL_CERTIFICATE_URI_COLUMN = "var_tal_certificate_uri";

	/**
	 * Mapping of the {@link ValidationRun} properties to its corresponding DB
	 * column
	 */
	public static final Map<String, String> propertyToColumnMap;
	static {
		propertyToColumnMap = new HashMap<>();
		propertyToColumnMap.put(ID, ID_COLUMN);
		propertyToColumnMap.put(UPDATED_AT, UPDATED_AT_COLUMN);
		propertyToColumnMap.put(COMPLETED_AT, COMPLETED_AT_COLUMN);
		propertyToColumnMap.put(STATUS, STATUS_COLUMN);
		propertyToColumnMap.put(TYPE, TYPE_COLUMN);
		propertyToColumnMap.put(TAL_ID, TAL_ID_COLUMN);
		propertyToColumnMap.put(TAL_CERTIFICATE_URI, TAL_CERTIFICATE_URI_COLUMN);
	}

	public ValidationRunDbObject(Type type) {
		super(type);
	}

	public ValidationRunDbObject(ValidationRun validationRun) {
		super(validationRun.getType());
		this.setId(validationRun.getId());
		this.setUpdatedAt(validationRun.getUpdatedAt());
		this.setCompletedAt(validationRun.getCompletedAt());
		this.setStatus(validationRun.getStatus());
		this.setTalId(validationRun.getTalId());
		this.setTalCertificateURI(validationRun.getTalCertificateURI());
		this.setRpkiRepositories(validationRun.getRpkiRepositories());
		this.setValidationChecks(validationRun.getValidationChecks());
	}

	public ValidationRunDbObject(ResultSet resultSet) throws SQLException {
		super(DatabaseObject.getStringAsEnum(Type.class, resultSet.getString(TYPE_COLUMN)));
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setUpdatedAt(DatabaseObject.getStringDateAsInstant(resultSet.getString(UPDATED_AT_COLUMN)));
		setCompletedAt(DatabaseObject.getStringDateAsInstant(resultSet.getString(COMPLETED_AT_COLUMN)));
		setStatus(DatabaseObject.getStringAsEnum(Status.class, resultSet.getString(STATUS_COLUMN)));
		setTalId(resultSet.getLong(TAL_ID_COLUMN));
		if (resultSet.wasNull()) {
			setTalId(null);
		}
		setTalCertificateURI(resultSet.getString(TAL_CERTIFICATE_URI_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		// updatedAt
		statement.setString(1, Instant.now().toString());
		if (getCompletedAt() != null) {
			statement.setString(2, getCompletedAt().toString());
		} else {
			statement.setNull(2, Types.VARCHAR);
		}
		if (getStatus() != null) {
			statement.setString(3, getStatus().toString());
		} else {
			statement.setNull(3, Types.VARCHAR);
		}
		if (getType() != null) {
			statement.setString(4, getType().toString());
		} else {
			statement.setNull(4, Types.VARCHAR);
		}
		if (getTalId() != null) {
			statement.setLong(5, getTalId());
		} else {
			statement.setNull(5, Types.NUMERIC);
		}
		if (getTalCertificateURI() != null) {
			statement.setString(6, getTalCertificateURI());
		} else {
			statement.setNull(6, Types.VARCHAR);
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
}
