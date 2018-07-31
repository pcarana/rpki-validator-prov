package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.http.HttpException;
import mx.nic.lab.rpki.db.exception.http.NotFoundException;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.db.spi.SlurmPrefixDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.SlurmPrefixModel;
import mx.nic.lab.rpki.sqlite.object.SlurmPrefixDbObject;
import mx.nic.lab.rpki.sqlite.object.DatabaseObject.Operation;

/**
 * Implementation to retrieve SLURM Prefix data
 *
 */
public class SlurmPrefixDAOImpl implements SlurmPrefixDAO {

	@Override
	public SlurmPrefix getById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<SlurmPrefix> getAll() throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getAll(connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public List<SlurmPrefix> getAllByType(int type) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getAllByType(connection, type);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public SlurmPrefix create(SlurmPrefix newSlurmPrefix) throws ApiDataAccessException {		
		SlurmPrefixDbObject slurmPrefixDb = new SlurmPrefixDbObject(newSlurmPrefix);
		slurmPrefixDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object doesn't exists
			if (SlurmPrefixModel.exist(newSlurmPrefix, connection)) {
				throw new HttpException(409, "The object already exists");
			}
			return SlurmPrefixModel.create(newSlurmPrefix, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean deleteById(Long id) throws ApiDataAccessException {
		// First check that the object actually exists
		try (Connection connection = DatabaseSession.getConnection()) {
			SlurmPrefix prefix = SlurmPrefixModel.getById(id, connection);
			if (prefix == null) {
				throw new NotFoundException();
			}
			int deleted = SlurmPrefixModel.deleteById(id, connection);
			return deleted > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}
}
