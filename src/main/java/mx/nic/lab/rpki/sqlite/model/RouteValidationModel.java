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

/**
 * Model to validate a route fetching data from the database (ROAs) and applying
 * some logic independent from the database. This class uses {@link RoaModel}.
 *
 */
public class RouteValidationModel {

	/**
	 * Validates the route with the received parameters and return the validation
	 * state following the RFC 6483 section-2 indications.
	 * 
	 * @param asn
	 * @param prefix
	 * @param prefixLength
	 * @param connection
	 * @return The {@link RouteValidation} with the result of the validation
	 * @throws SQLException
	 */
	public static RouteValidation validate(Long asn, byte[] prefix, Integer prefixLength, Connection connection)
			throws SQLException {
		// FIXME Consider SLURM
		// Go for the exact match
		Roa matchedRoa = RoaModel.findExactMatch(prefix, prefixLength, connection);
		if (matchedRoa != null) {
			boolean asnMatch = asn.equals(matchedRoa.getAsn());
			ValidityState validityState = asnMatch ? ValidityState.VALID : ValidityState.INVALID;
			return createRouteValidation(validityState, PrefixState.MATCH_ROA, asnMatch, matchedRoa);
		}
		// Check if there's a ROA covering the received prefix (a.k.a the received
		// prefix is more specific than ROA)
		List<Roa> candidateRoas = RoaModel.findCoveringAggregate(prefix, prefixLength, connection);
		for (Roa roa : candidateRoas) {
			// The prefix is effectively a son of the ROA
			if (!isPrefixInRange(prefix, roa.getStartPrefix(), roa.getPrefixLength())) {
				continue;
			}
			return createRouteValidation(ValidityState.INVALID, PrefixState.MORE_SPECIFIC, asn.equals(roa.getAsn()),
					roa);
		}
		// Check if there's a ROA more specific (a.k.a the received prefix is a
		// covering aggregate of the ROA)
		candidateRoas = RoaModel.findMoreSpecific(prefix, prefixLength, connection);
		for (Roa roa : candidateRoas) {
			// The ROA is effectively a son of the prefix
			if (!isPrefixInRange(roa.getStartPrefix(), prefix, prefixLength)) {
				continue;
			}
			return createRouteValidation(ValidityState.UNKNOWN, PrefixState.COVERING_AGGREGATE,
					asn.equals(roa.getAsn()), roa);
		}
		// No match at all, check if at least oen ROA exists with the ASN
		return createRouteValidation(ValidityState.UNKNOWN, PrefixState.NON_INTERSECTING,
				RoaModel.existAsn(asn, connection), null);
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
	 * use the <code>asnMatch<code> to determine the {@link AsState}.
	 * 
	 * @param validityState
	 * @param prefixState
	 * @param asnMatch
	 * @param matchRoa
	 * @return
	 */
	private static RouteValidation createRouteValidation(ValidityState validityState, PrefixState prefixState,
			boolean asnMatch, Roa matchRoa) {
		RouteValidation result = new RouteValidation();
		result.setValidityState(validityState);
		result.setPrefixState(prefixState);
		result.setAsState(asnMatch ? AsState.MATCHING : AsState.NON_MATCHING);
		result.setRoaMatch(matchRoa);
		return result;
	}
}
