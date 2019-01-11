package mx.nic.lab.rpki.sqlite.model;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.pojo.RouteValidation;
import mx.nic.lab.rpki.db.pojo.RouteValidation.AsState;
import mx.nic.lab.rpki.db.pojo.RouteValidation.PrefixState;
import mx.nic.lab.rpki.db.pojo.RouteValidation.ValidityState;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;

/**
 * Model to validate a route fetching data from the database (ROAs) and applying
 * some logic independent from the database. This class uses {@link RoaModel}
 * and {@link SlurmPrefixModel}.
 *
 */
public class RouteValidationModel {

	/**
	 * Validates the route with the received parameters and return the validation
	 * state following the RFC 6483 section-2 indications and the configured SLURM.
	 * 
	 * @param asn
	 * @param prefix
	 * @param prefixLength
	 * @param familyType
	 * @param fullCheck
	 * @param connection
	 * @return The {@link RouteValidation} with the result of the validation
	 * @throws SQLException
	 */
	public static RouteValidation validate(Long asn, byte[] prefix, Integer prefixLength, Integer familyType,
			boolean fullCheck, Connection connection) throws SQLException {
		// If there's an assertion then stop the search and return the assertion result
		RouteValidation slurmValidation = findSlurmAssertion(asn, prefix, prefixLength, fullCheck, connection);
		if (slurmValidation != null) {
			return slurmValidation;
		}
		// No assertion, check if there's a filter
		slurmValidation = findSlurmFilter(asn, prefix, prefixLength, fullCheck, connection);
		if (slurmValidation != null) {
			return slurmValidation;
		}
		// Well, then go for the ROA match(es)
		return findRoaValidation(asn, prefix, prefixLength, familyType, fullCheck, connection);

	}

	/**
	 * Look for a SLURM assertion that matches the prefix, the SLURM assertion is
	 * treated just as a valid ROA
	 * 
	 * @param asn
	 * @param prefix
	 * @param prefixLength
	 * @param fullCheck
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static RouteValidation findSlurmAssertion(Long asn, byte[] prefix, Integer prefixLength, boolean fullCheck,
			Connection connection) throws SQLException {
		// Go for the exact SLURM prefix assertion match
		SlurmPrefix matchedSlurmPrefix = SlurmPrefixModel.findExactMatch(prefix, prefixLength, connection);
		if (matchedSlurmPrefix != null) {
			boolean asnMatch = asn.equals(matchedSlurmPrefix.getAsn());
			ValidityState validityState = asnMatch ? ValidityState.VALID : ValidityState.INVALID;
			AsState asState = asnMatch ? AsState.MATCHING : AsState.NON_MATCHING;
			return createSlurmRouteValidation(validityState, PrefixState.MATCH_ROA, asState, matchedSlurmPrefix,
					fullCheck);
		}
		// Check if there's a SLURM prefix assertion covering the received prefix (a.k.a
		// the
		// received prefix is more specific than SLURM prefix)
		List<SlurmPrefix> candidatePrefixes = SlurmPrefixModel.findCoveringAggregate(prefix, prefixLength, connection);
		for (SlurmPrefix slurmPrefix : candidatePrefixes) {
			// The prefix is effectively a son of the SLURM prefix
			if (!isPrefixInRange(prefix, slurmPrefix.getStartPrefix(), slurmPrefix.getPrefixLength())) {
				continue;
			}
			AsState asState = asn.equals(slurmPrefix.getAsn()) ? AsState.MATCHING : AsState.NON_MATCHING;
			return createSlurmRouteValidation(ValidityState.INVALID, PrefixState.MORE_SPECIFIC, asState, slurmPrefix,
					fullCheck);
		}
		// Check if there's a SLURM prefix more specific (a.k.a the received prefix is a
		// covering aggregate of the SLURM prefix)
		candidatePrefixes = SlurmPrefixModel.findMoreSpecific(prefix, prefixLength, connection);
		for (SlurmPrefix slurmPrefix : candidatePrefixes) {
			// The SLURM prefix is effectively a son of the prefix
			if (!isPrefixInRange(slurmPrefix.getStartPrefix(), prefix, prefixLength)) {
				continue;
			}
			AsState asState = asn.equals(slurmPrefix.getAsn()) ? AsState.MATCHING : AsState.NON_MATCHING;
			return createSlurmRouteValidation(ValidityState.UNKNOWN, PrefixState.COVERING_AGGREGATE, asState,
					slurmPrefix, fullCheck);
		}
		// There's no "UNKNOWN" case for SLURM assertions, return null to search for
		// real ROAs
		return null;
	}

	/**
	 * Check if there's a SLURM filter that "filters" the received prefix
	 * 
	 * @param asn
	 * @param prefix
	 * @param prefixLength
	 * @param fullCheck
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static RouteValidation findSlurmFilter(Long asn, byte[] prefix, Integer prefixLength, boolean fullCheck,
			Connection connection) throws SQLException {
		// Search if there's any filter that matches the request
		SlurmPrefix matchedFilter = SlurmPrefixModel.findFilterMatch(asn, prefix, prefixLength, connection);
		if (matchedFilter != null) {
			return createSlurmRouteValidation(ValidityState.UNKNOWN, PrefixState.NON_INTERSECTING, AsState.NON_MATCHING,
					matchedFilter, fullCheck);
		}
		return null;
	}

	/**
	 * Find if there's a ROA that matches the received prefix
	 * 
	 * @param asn
	 * @param prefix
	 * @param prefixLength
	 * @param familyType
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static RouteValidation findRoaValidation(Long asn, byte[] prefix, Integer prefixLength, Integer familyType,
			boolean fullCheck, Connection connection) throws SQLException {
		// Go for the exact ROA match
		Roa matchedRoa = RoaModel.findExactMatch(prefix, prefixLength, connection);
		if (matchedRoa != null) {
			boolean asnMatch = asn.equals(matchedRoa.getAsn());
			ValidityState validityState = asnMatch ? ValidityState.VALID : ValidityState.INVALID;
			AsState asState = asnMatch ? AsState.MATCHING : AsState.NON_MATCHING;
			return createRoaRouteValidation(validityState, PrefixState.MATCH_ROA, asState, matchedRoa, true);
		}
		if (!fullCheck) {
			return createRoaRouteValidation(null, null, null, null, fullCheck);
		}
		// Check if there's a ROA covering the received prefix (a.k.a the received
		// prefix is more specific than ROA)
		List<Roa> candidateRoas = RoaModel.findCoveringAggregate(prefix, prefixLength, familyType, connection);
		for (Roa roa : candidateRoas) {
			// The prefix is effectively a son of the ROA
			if (!isPrefixInRange(prefix, roa.getStartPrefix(), roa.getPrefixLength())) {
				continue;
			}
			AsState asState = asn.equals(roa.getAsn()) ? AsState.MATCHING : AsState.NON_MATCHING;
			roa = RoaModel.getById(roa.getId(), connection);
			return createRoaRouteValidation(ValidityState.INVALID, PrefixState.MORE_SPECIFIC, asState, roa, fullCheck);
		}
		// Check if there's a ROA more specific (a.k.a the received prefix is a
		// covering aggregate of the ROA)
		candidateRoas = RoaModel.findMoreSpecific(prefix, prefixLength, familyType, connection);
		for (Roa roa : candidateRoas) {
			// The ROA is effectively a son of the prefix
			if (!isPrefixInRange(roa.getStartPrefix(), prefix, prefixLength)) {
				continue;
			}
			AsState asState = asn.equals(roa.getAsn()) ? AsState.MATCHING : AsState.NON_MATCHING;
			roa = RoaModel.getById(roa.getId(), connection);
			return createRoaRouteValidation(ValidityState.UNKNOWN, PrefixState.COVERING_AGGREGATE, asState, roa,
					fullCheck);
		}
		// No match at all, check if at least oen ROA exists with the ASN
		boolean asnMatch = RoaModel.existAsn(asn, connection);
		AsState asState = asnMatch ? AsState.MATCHING : AsState.NON_MATCHING;
		return createRoaRouteValidation(ValidityState.UNKNOWN, PrefixState.NON_INTERSECTING, asState, null, fullCheck);
	}

	/**
	 * Check if the <code>sonPrefix</code> is the same range that the
	 * <code>fatherPrefix</code>, using the prefix length of the father
	 * (<code>fatherLength</code>).
	 * 
	 * @param sonPrefix
	 * @param fatherPrefix
	 * @param fatherLength
	 * @return
	 */
	private static boolean isPrefixInRange(byte[] sonPrefix, byte[] fatherPrefix, Integer fatherLength) {
		// Both prefix are of the same IP type
		if (sonPrefix.length != fatherPrefix.length) {
			return false;
		}
		int bytesBase = fatherLength / 8;
		int bitsBase = fatherLength % 8;
		byte[] prefixLengthMask = new byte[fatherPrefix.length];
		int currByte = 0;
		for (; currByte < bytesBase; currByte++) {
			prefixLengthMask[currByte] |= 255;
		}
		if (currByte < prefixLengthMask.length) {
			prefixLengthMask[currByte] = (byte) (255 << (8 - bitsBase));
		}
		BigInteger sonIp = new BigInteger(sonPrefix);
		BigInteger fatherIp = new BigInteger(fatherPrefix);
		BigInteger mask = new BigInteger(prefixLengthMask);
		return sonIp.and(mask).equals(fatherIp);
	}

	/**
	 * Create a new instance of {@link RouteValidation} with the specified values,
	 * considering that the object matched is a {@link Roa}; use the
	 * <code>asnMatch<code> to determine the {@link AsState}.
	 * 
	 * @param validityState
	 * @param prefixState
	 * @param asState
	 * @param matchRoa
	 * @param fullCheck
	 * @return
	 */
	private static RouteValidation createRoaRouteValidation(ValidityState validityState, PrefixState prefixState,
			AsState asState, Roa matchRoa, boolean fullCheck) {
		RouteValidation result = new RouteValidation();
		result.setValidityState(validityState);
		result.setPrefixState(prefixState);
		result.setAsState(asState);
		result.setRoaMatch(matchRoa);
		result.setFullCheck(fullCheck);
		return result;
	}

	/**
	 * Create a new instance of {@link RouteValidation} with the specified values,
	 * considering that the object matched is a {@link SlurmPrefix}; use the
	 * <code>asnMatch<code> to determine the {@link AsState}.
	 * 
	 * @param validityState
	 * @param prefixState
	 * @param asState
	 * @param matchSlurmPrefix
	 * @param fullCheck
	 * @return
	 */
	private static RouteValidation createSlurmRouteValidation(ValidityState validityState, PrefixState prefixState,
			AsState asState, SlurmPrefix matchSlurmPrefix, boolean fullCheck) {
		RouteValidation result = new RouteValidation();
		result.setValidityState(validityState);
		result.setPrefixState(prefixState);
		result.setAsState(asState);
		result.setSlurmMatch(matchSlurmPrefix);
		result.setFullCheck(fullCheck);
		return result;
	}
}
