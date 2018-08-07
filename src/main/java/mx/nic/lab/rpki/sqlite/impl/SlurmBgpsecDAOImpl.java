package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.SlurmBgpsec;
import mx.nic.lab.rpki.db.spi.SlurmBgpsecDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.SlurmBgpsecModel;
import mx.nic.lab.rpki.sqlite.object.DatabaseObject.Operation;
import mx.nic.lab.rpki.sqlite.object.SlurmBgpsecDbObject;

/**
 * Implementation to retrieve SLURM BGPsec data
 *
 */
public class SlurmBgpsecDAOImpl implements SlurmBgpsecDAO {

	@Override
	public SlurmBgpsec getById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmBgpsecModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<SlurmBgpsec> getAll() throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmBgpsecModel.getAll(connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<SlurmBgpsec> getAllByType(int type) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmBgpsecModel.getAllByType(connection, type);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public SlurmBgpsec create(SlurmBgpsec newSlurmBgpsec) throws ApiDataAccessException {
		SlurmBgpsecDbObject slurmBgpsecDb = new SlurmBgpsecDbObject(newSlurmBgpsec);
		slurmBgpsecDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object doesn't exists
			if (SlurmBgpsecModel.exist(newSlurmBgpsec, connection)) {
				throw new ValidationException(
						new ValidationError(SlurmBgpsec.OBJECT_NAME, ValidationErrorType.OBJECT_EXISTS));
			}
			return SlurmBgpsecModel.create(newSlurmBgpsec, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
