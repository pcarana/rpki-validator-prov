package mx.nic.lab.rpki.sqlite.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import mx.nic.lab.rpki.db.pojo.Slurm;
import mx.nic.lab.rpki.db.pojo.SlurmBgpsec;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;

/**
 * Model to retrieve SLURM complete data from the database (prefixes and
 * BGPsec). This class uses the implementations of {@link SlurmPrefixModel} and
 * {@link SlurmBgpsecModel}.
 *
 */
public class SlurmModel {

	/**
	 * Get the complete {@link Slurm}, with both {@link SlurmPrefix} and
	 * {@link SlurmBgpsec} lists.
	 * 
	 * @param connection
	 * @return The complete SLURM, or null if no data is found
	 * @throws SQLException
	 */
	public static Slurm getAll(Connection connection) throws SQLException {
		List<SlurmPrefix> prefixes = SlurmPrefixModel.getAll(-1, -1, null, connection);
		List<SlurmBgpsec> bgpsecs = SlurmBgpsecModel.getAll(-1, -1, null, connection);
		if (prefixes.isEmpty() && bgpsecs.isEmpty()) {
			return null;
		}
		Slurm slurm = new Slurm();
		slurm.setPrefixes(prefixes);
		slurm.setBgpsecs(bgpsecs);

		return slurm;
	}
}
