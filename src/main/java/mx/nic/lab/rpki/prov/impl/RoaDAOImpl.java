package mx.nic.lab.rpki.prov.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.spi.RoaDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.RoaModel;

/**
 * Implementation to retrieve ROAs data
 *
 */
public class RoaDAOImpl implements RoaDAO {

	@Override
	public Roa getById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RoaModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public ListResult<Roa> getAll(PagingParameters pagingParams) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RoaModel.getAll(pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
