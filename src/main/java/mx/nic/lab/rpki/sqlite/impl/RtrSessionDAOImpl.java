package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.RtrSession;
import mx.nic.lab.rpki.db.spi.RtrSessionDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.RtrSessionModel;

/**
 * Implementation to retrieve the RTR sessions
 *
 */
public class RtrSessionDAOImpl implements RtrSessionDAO {

	@Override
	public List<RtrSession> getAll(int limit, int offset, LinkedHashMap<String, String> sort)
			throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RtrSessionModel.getAll(limit, offset, sort, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
