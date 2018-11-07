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
		List<SlurmPrefix> prefixes = SlurmPrefixModel.getAll(null, connection).getResults();
		List<SlurmBgpsec> bgpsecs = SlurmBgpsecModel.getAll(null, connection).getResults();
		if (prefixes.isEmpty() && bgpsecs.isEmpty()) {
			return null;
		}
		Slurm slurm = new Slurm();
		prefixes.forEach((prefix) -> {
			if (prefix.getType().equals(SlurmPrefix.TYPE_ASSERTION)) {
				slurm.getLocallyAddedAssertions().getPrefixes().add(prefix);
			} else {
				slurm.getValidationOutputFilters().getPrefixes().add(prefix);
			}
		});
		bgpsecs.forEach((bgpsec) -> {
			if (bgpsec.getType().equals(SlurmBgpsec.TYPE_ASSERTION)) {
				slurm.getLocallyAddedAssertions().getBgpsecs().add(bgpsec);
			} else {
				slurm.getValidationOutputFilters().getBgpsecs().add(bgpsec);
			}
		});
		return slurm;
	}
}
