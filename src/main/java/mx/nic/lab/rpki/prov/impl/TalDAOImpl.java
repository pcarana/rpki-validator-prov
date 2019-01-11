package mx.nic.lab.rpki.prov.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.db.spi.TalDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.TalModel;
import mx.nic.lab.rpki.prov.object.TalDbObject;
import mx.nic.lab.rpki.prov.object.DatabaseObject.Operation;

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
	public ListResult<Tal> getAll(PagingParameters pagingParams) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.getAll(pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Tal getExistentTal(Tal tal) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.getExistentTal(tal, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Long create(Tal tal) throws ApiDataAccessException {
		TalDbObject talDb = new TalDbObject(tal);
		talDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.create(tal, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean delete(Tal tal) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			int deleted = TalModel.delete(tal, connection);
			return deleted > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean updateLoadedCertificate(Tal tal) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			int updated = TalModel.updateLoadedCertificate(tal, connection);
			return updated > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
