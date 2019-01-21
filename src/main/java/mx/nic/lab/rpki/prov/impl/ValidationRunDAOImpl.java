package mx.nic.lab.rpki.prov.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.ValidationCheck;
import mx.nic.lab.rpki.db.pojo.ValidationCheck.Status;
import mx.nic.lab.rpki.db.pojo.ValidationRun;
import mx.nic.lab.rpki.db.spi.ValidationRunDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.ValidationCheckModel;
import mx.nic.lab.rpki.prov.model.ValidationRunModel;
import mx.nic.lab.rpki.prov.object.DatabaseObject.Operation;
import mx.nic.lab.rpki.prov.object.ValidationRunDbObject;

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

	@Override
	public ListResult<ValidationCheck> getLastSuccessfulChecksByTal(Long talId, PagingParameters pagingParams)
			throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return ValidationCheckModel.getLastSuccessfulChecksByTal(talId, pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Map<Status, Map<String, Long>> getLastSuccessfulCheckSummByTal(Long talId) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return ValidationCheckModel.getLastSuccessfulChecksSummByTal(talId, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
