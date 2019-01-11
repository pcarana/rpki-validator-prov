package mx.nic.lab.rpki.prov.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.cert.tree.CertificationTreeNode;
import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.Tal;
import mx.nic.lab.rpki.db.spi.CertificateTreeDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.CertificateTreeModel;
import mx.nic.lab.rpki.prov.model.TalModel;

/**
 * Implementation to retrieve the Certificate Tree
 *
 */
public class CertificateTreeDAOImpl implements CertificateTreeDAO {

	@Override
	public CertificationTreeNode getFromRoot(Long talId) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			Tal tal = TalModel.getById(talId, connection);
			if (tal == null) {
				return null;
			}
			return CertificateTreeModel.findFromRoot(tal, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public CertificationTreeNode getFromChild(Long certId) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return CertificateTreeModel.findFromChild(certId, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
