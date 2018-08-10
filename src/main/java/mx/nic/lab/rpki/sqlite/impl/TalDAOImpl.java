package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.db.spi.TalDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.TalModel;

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
	public List<Tal> getAll(int limit, int offset, LinkedHashMap<String, String> sort) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return TalModel.getAll(limit, offset, sort, connection);
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

}
