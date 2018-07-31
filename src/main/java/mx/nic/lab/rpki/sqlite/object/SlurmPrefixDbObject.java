package mx.nic.lab.rpki.sqlite.object;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.http.BadRequestException;
import mx.nic.lab.rpki.db.exception.http.HttpException;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;

/**
 * Extension of {@link SlurmPrefix} as a {@link DatabaseObject}
 *
 */
public class SlurmPrefixDbObject extends SlurmPrefix implements DatabaseObject {

	public static final String ID_COLUMN = "slp_id";
	public static final String ASN_COLUMN = "slp_asn";
	public static final String PREFIX_TEXT_COLUMN = "slp_prefix_text";
	public static final String START_PREFIX_COLUMN = "slp_start_prefix";
	public static final String END_PREFIX_COLUMN = "slp_end_prefix";
	public static final String PREFIX_LENGTH_COLUMN = "slp_prefix_length";
	public static final String PREFIX_MAX_LENGTH_COLUMN = "slp_prefix_max_length";
	public static final String TYPE_COLUMN = "slp_type";
	public static final String COMMENT_COLUMN = "slp_comment";

	public SlurmPrefixDbObject() {
		super();
	}

	/**
	 * Build an instance from a {@link SlurmPrefix}
	 * 
	 * @param slurmPrefix
	 */
	public SlurmPrefixDbObject(SlurmPrefix slurmPrefix) {
		this.setId(slurmPrefix.getId());
		this.setAsn(slurmPrefix.getAsn());
		this.setPrefixText(slurmPrefix.getPrefixText());
		this.setStartPrefix(slurmPrefix.getStartPrefix());
		this.setEndPrefix(slurmPrefix.getEndPrefix());
		this.setPrefixLength(slurmPrefix.getPrefixLength());
		this.setPrefixMaxLength(slurmPrefix.getPrefixMaxLength());
		this.setType(slurmPrefix.getType());
		this.setComment(slurmPrefix.getComment());
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public SlurmPrefixDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setAsn(resultSet.getLong(ASN_COLUMN));
		if (resultSet.wasNull()) {
			setAsn(null);
		}
		setPrefixText(resultSet.getString(PREFIX_TEXT_COLUMN));
		setStartPrefix(resultSet.getBytes(START_PREFIX_COLUMN));
		if (resultSet.wasNull()) {
			setStartPrefix(null);
		}
		setEndPrefix(resultSet.getBytes(END_PREFIX_COLUMN));
		if (resultSet.wasNull()) {
			setEndPrefix(null);
		}
		setPrefixLength(resultSet.getInt(PREFIX_LENGTH_COLUMN));
		if (resultSet.wasNull()) {
			setPrefixLength(null);
		}
		setPrefixMaxLength(resultSet.getInt(PREFIX_MAX_LENGTH_COLUMN));
		if (resultSet.wasNull()) {
			setPrefixMaxLength(null);
		}
		setType(resultSet.getInt(TYPE_COLUMN));
		if (resultSet.wasNull()) {
			setType(null);
		}
		setComment(resultSet.getString(COMMENT_COLUMN));
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		statement.setLong(1, getId());
		if (getAsn() != null) {
			statement.setLong(2, getAsn());
		} else {
			statement.setNull(2, Types.NUMERIC);
		}
		if (getPrefixText() != null) {
			statement.setString(3, getPrefixText());
		} else {
			statement.setNull(3, Types.VARCHAR);
		}
		if (getStartPrefix() != null) {
			statement.setBytes(4, getStartPrefix());
		} else {
			statement.setNull(4, Types.BLOB);
		}
		if (getEndPrefix() != null) {
			statement.setBytes(5, getEndPrefix());
		} else {
			statement.setNull(5, Types.BLOB);
		}
		if (getPrefixLength() != null) {
			statement.setInt(6, getPrefixLength());
		} else {
			statement.setNull(6, Types.INTEGER);
		}
		if (getPrefixMaxLength() != null) {
			statement.setInt(7, getPrefixMaxLength());
		} else {
			statement.setNull(7, Types.INTEGER);
		}
		if (getType() != null) {
			statement.setInt(8, getType());
		} else {
			statement.setNull(8, Types.INTEGER);
		}
		if (getComment() != null) {
			statement.setString(9, getComment());
		} else {
			statement.setNull(9, Types.VARCHAR);
		}
	}

	@Override
	public void validate(Operation operation) throws ApiDataAccessException {
		if (operation == Operation.CREATE) {
			// Check the attributes according to the type
			Integer type = this.getType();
			Long asn = this.getAsn();
			byte[] startPrefix = this.getStartPrefix();
			byte[] endPrefix = this.getEndPrefix();
			Integer prefixLength = this.getPrefixLength();
			Integer prefixMaxLength = this.getPrefixMaxLength();
			String comment = this.getComment();

			// The ID isn't validated since this is a new object

			if (type == null || (type != SlurmPrefix.TYPE_FILTER && type != SlurmPrefix.TYPE_ASSERTION)) {
				throw new BadRequestException("Invalid type");
			}

			if (type == SlurmPrefix.TYPE_FILTER) {
				// Required values
				// Either an ASN or a Prefix must exist
				if (asn == null && startPrefix == null) {
					throw new BadRequestException("ASN and/or prefix null");
				}
				// Max length is only for assertions
				if (prefixMaxLength != null) {
					throw new BadRequestException("Max length must be null");
				}
			} else if (type == SlurmPrefix.TYPE_ASSERTION) {
				// Both ASN and a Prefix must exist
				if (asn == null || startPrefix == null) {
					throw new BadRequestException("ASN and prefix null");
				}
			}
			// "It is RECOMMENDED that an explanatory comment is also included"
			// (draft-ietf-sidr-slurm-08)
			if (comment == null || comment.trim().isEmpty()) {
				throw new BadRequestException("Comment null");
			} else if (!(comment.trim().length() > 0 && comment.trim().length() <= 2000)) {
				// MAX 2000 (randomly picked to avoid abuse)
				throw new BadRequestException("Comment out of range");
			}
			// Prefix and prefix length must be together
			if ((startPrefix != null && prefixLength == null) || (startPrefix == null && prefixLength != null)) {
				throw new BadRequestException("Prefix incomplete");
			}
			if (asn != null && !(asn >= 0L && asn <= 4294967295L)) {
				throw new BadRequestException("ASN out of range");
			}
			if (prefixLength != null) {
				validatePrefixLength(startPrefix, prefixLength, "Prefix length out of range");
				// If max length is indicated, it can't be grater than length
				if (prefixMaxLength != null && prefixMaxLength < prefixLength) {
					throw new BadRequestException("Prefix max length can't be greater than prefix length");
				}
			}
			if (prefixMaxLength != null) {
				validatePrefixLength(startPrefix, prefixMaxLength, "Prefix max length out of range");
			}
			// If there's a Start Prefix, then it must exist an End Prefix
			if (startPrefix != null && endPrefix == null) {
				throw new BadRequestException("Invalid end prefix");
			}
		}
	}

	private void validatePrefixLength(byte[] prefix, int prefixLength, String errorMessage) throws HttpException {
		// Prefix length according to IP type
		boolean isIpv4 = false;
		int min = 0;
		int max = 128;
		try {
			InetAddress prefixAddress = InetAddress.getByAddress(prefix);
			isIpv4 = prefixAddress instanceof Inet4Address;
		} catch (UnknownHostException e) {
			throw new BadRequestException("Invalid prefix");
		}
		if (isIpv4) {
			max = 32;
		}
		if (!(prefixLength >= min && prefixLength <= max)) {
			throw new BadRequestException(errorMessage);
		}
	}
}
