package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.ValidationCheck;

public class ValidationCheckDbObject extends ValidationCheck implements DatabaseObject {

	public static final String ID_COLUMN = "vac_id";
	public static final String VALIDATION_RUN_COLUMN = "var_id";
	public static final String LOCATION_COLUMN = "vac_location";
	public static final String STATUS_COLUMN = "vac_status";
	public static final String KEY_COLUMN = "vac_key";
	public static final String PARAMETERS_COLUMN = "vcp_parameters";

	public ValidationCheckDbObject() {
		super();
	}

	public ValidationCheckDbObject(ValidationCheck validationCheck) {
		this.setId(validationCheck.getId());
		this.setValidationRunId(validationCheck.getValidationRunId());
		this.setLocation(validationCheck.getLocation());
		this.setStatus(validationCheck.getStatus());
		this.setKey(validationCheck.getKey());
		this.setParameters(validationCheck.getParameters());
	}

	public ValidationCheckDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setValidationRunId(resultSet.getLong(VALIDATION_RUN_COLUMN));
		if (resultSet.wasNull()) {
			setValidationRunId(null);
		}
		setLocation(resultSet.getString(LOCATION_COLUMN));
		setStatus(DatabaseObject.getStringAsEnum(Status.class, resultSet.getString(STATUS_COLUMN)));
		setKey(resultSet.getString(KEY_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		if (getValidationRunId() != null) {
			statement.setLong(1, getValidationRunId());
		} else {
			statement.setNull(1, Types.NUMERIC);
		}
		if (getLocation() != null) {
			statement.setString(2, getLocation());
		} else {
			statement.setNull(2, Types.VARCHAR);
		}
		if (getStatus() != null) {
			statement.setString(3, getStatus().toString());
		} else {
			statement.setNull(3, Types.VARCHAR);
		}
		if (getKey() != null) {
			statement.setString(4, getKey());
		} else {
			statement.setNull(4, Types.VARCHAR);
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
