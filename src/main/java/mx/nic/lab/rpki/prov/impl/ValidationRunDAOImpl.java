package mx.nic.lab.rpki.prov.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.ValidationRun;
import mx.nic.lab.rpki.db.spi.ValidationRunDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.ValidationRunModel;
import mx.nic.lab.rpki.prov.object.ValidationRunDbObject;
import mx.nic.lab.rpki.prov.object.DatabaseObject.Operation;

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
		boolean result = false;
		try (Connection connection = DatabaseSession.getConnection()) {
			int updated = ValidationRunModel.completeValidation(validationRun, connection);
			result = updated > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
		// And remove the older ones
		try (Connection connection = DatabaseSession.getConnection()) {
			ValidationRunModel.deleteOldValidationRuns(validationRun, connection);
			return result;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
