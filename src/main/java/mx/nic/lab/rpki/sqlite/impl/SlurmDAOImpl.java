package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.Slurm;
import mx.nic.lab.rpki.db.spi.SlurmDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.SlurmModel;

/**
 * Implementation to retrieve the complete SLURM
 *
 */
public class SlurmDAOImpl implements SlurmDAO {

	@Override
	public Slurm getAll() throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmModel.getAll(connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}