package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.ValidationRun;
import mx.nic.lab.rpki.db.spi.ValidationRunDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.ValidationRunModel;
import mx.nic.lab.rpki.sqlite.object.DatabaseObject.Operation;
import mx.nic.lab.rpki.sqlite.object.ValidationRunDbObject;

/**
 * Implementation to retrieve the Validation runs
 *
 */
public class ValidationRunDAOImpl implements ValidationRunDAO {

	@Override
	public Long create(ValidationRun validationRun) throws ApiDataAccessException {
		ValidationRunDbObject validationRunDb = new ValidationRunDbObject(validationRun);
		validationRunDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			return ValidationRunModel.create(validationRun, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean completeValidation(ValidationRun validationRun) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object exists
			if (ValidationRunModel.getById(validationRun.getId(), connection) == null) {
				throw new ValidationException(
						new ValidationError(ValidationRun.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			int updated = ValidationRunModel.completeValidation(validationRun, connection);
			return updated > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public ValidationRun get(long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return ValidationRunModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<ValidationRun> findAll() throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return ValidationRunModel.getAll(null, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public long removeOldValidationRuns(Instant completedBefore) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return ValidationRunModel.deleteOldValidationRuns(completedBefore, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
