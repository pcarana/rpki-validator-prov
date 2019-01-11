package mx.nic.lab.rpki.prov.impl;

import java.util.Properties;

import mx.nic.lab.rpki.db.exception.InitializationException;
import mx.nic.lab.rpki.db.spi.CertificateTreeDAO;
import mx.nic.lab.rpki.db.spi.DataAccessImplementation;
import mx.nic.lab.rpki.db.spi.RoaDAO;
import mx.nic.lab.rpki.db.spi.RouteValidationDAO;
import mx.nic.lab.rpki.db.spi.RpkiObjectDAO;
import mx.nic.lab.rpki.db.spi.RpkiRepositoryDAO;
import mx.nic.lab.rpki.db.spi.SlurmBgpsecDAO;
import mx.nic.lab.rpki.db.spi.SlurmDAO;
import mx.nic.lab.rpki.db.spi.SlurmPrefixDAO;
import mx.nic.lab.rpki.db.spi.TalDAO;
import mx.nic.lab.rpki.db.spi.ValidationRunDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.QueryLoader;

/**
 * Implementation used for {@link DataAccessImplementation}
 *
 */
public class ProviderImplementation implements DataAccessImplementation {

	@Override
	public void init(Properties properties) throws InitializationException {
		DatabaseSession.initConnection(properties);
		QueryLoader.init(properties);
	}

	@Override
	public void terminate() {
		DatabaseSession.endConnection();
	}

	@Override
	public TalDAO getTalDAO() {
		return new TalDAOImpl();
	}

	@Override
	public RoaDAO getRoaDAO() {
		return new RoaDAOImpl();
	}

	@Override
	public SlurmPrefixDAO getSlurmPrefixDAO() {
		return new SlurmPrefixDAOImpl();
	}

	@Override
	public SlurmBgpsecDAO getSlurmBgpsecDAO() {
		return new SlurmBgpsecDAOImpl();
	}

	@Override
	public SlurmDAO getSlurmDAO() {
		return new SlurmDAOImpl();
	}

	@Override
	public RouteValidationDAO getRouteValidationDAO() {
		return new RouteValidationDAOImpl();
	}

	@Override
	public RpkiObjectDAO getRpkiObjectDAO() {
		return new RpkiObjectDAOImpl();
	}

	@Override
	public RpkiRepositoryDAO getRpkiRepositoryDAO() {
		return new RpkiRepositoryDAOImpl();
	}

	@Override
	public ValidationRunDAO getValidationRunDAO() {
		return new ValidationRunDAOImpl();
	}

	@Override
	public CertificateTreeDAO getCertificateTreeDAO() {
		return new CertificateTreeDAOImpl();
	}

}
