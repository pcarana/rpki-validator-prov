package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.db.spi.TalDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.TalModel;
import mx.nic.lab.rpki.sqlite.object.DatabaseObject.Operation;
import mx.nic.lab.rpki.sqlite.object.TalDbObject;

/**
 * Implementation to retrieve TALs data
 *
 */
public class TalDAOImpl implements TalDAO {

	@Override
	public Tal getById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<Tal> getAll(PagingParameters pagingParams) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.getAll(pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Tal syncById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.syncById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<Tal> syncAll() throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.syncAll(connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Long create(Tal tal) throws ApiDataAccessException {
		TalDbObject talDb = new TalDbObject(tal);
		talDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object doesn't exists
			if (TalModel.exist(tal, connection)) {
				throw new ValidationException(new ValidationError(Tal.OBJECT_NAME, ValidationErrorType.OBJECT_EXISTS));
			}
			return TalModel.create(tal, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean delete(Tal tal) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object exists
			if (!TalModel.exist(tal, connection)) {
				throw new ValidationException(
						new ValidationError(Tal.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			int deleted = TalModel.delete(tal, connection);
			return deleted > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean updateLoadedCertificate(Tal tal) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object exists
			if (!TalModel.exist(tal, connection)) {
				throw new ValidationException(
						new ValidationError(Tal.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			int updated = TalModel.updateLoadedCertificate(tal, connection);
			return updated > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
