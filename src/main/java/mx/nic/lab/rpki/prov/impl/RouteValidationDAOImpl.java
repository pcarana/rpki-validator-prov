package mx.nic.lab.rpki.prov.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.RouteValidation;
import mx.nic.lab.rpki.db.spi.RouteValidationDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.RouteValidationModel;

/**
 * Implementation to validate a route
 *
 */
public class RouteValidationDAOImpl implements RouteValidationDAO {

	@Override
	public RouteValidation validate(Long asn, byte[] prefix, Integer prefixLength, Integer familyType,
			boolean fullCheck) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RouteValidationModel.validate(asn, prefix, prefixLength, familyType, fullCheck, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
