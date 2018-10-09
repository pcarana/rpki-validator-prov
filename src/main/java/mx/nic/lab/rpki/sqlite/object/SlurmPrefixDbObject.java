package mx.nic.lab.rpki.sqlite.object;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.ApiObject;
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

	public static final int COMMENT_MIN_LENGTH = 1;
	public static final int COMMENT_MAX_LENGTH = 2000;

	/**
	 * Mapping of the {@link SlurmPrefix} properties to its corresponding DB column
	 */
	public static final Map<String, String> propertyToColumnMap;
	static {
		propertyToColumnMap = new HashMap<>();
		propertyToColumnMap.put(ID, ID_COLUMN);
		propertyToColumnMap.put(ASN, ASN_COLUMN);
		propertyToColumnMap.put(PREFIX_TEXT, PREFIX_TEXT_COLUMN);
		propertyToColumnMap.put(START_PREFIX, START_PREFIX_COLUMN);
		propertyToColumnMap.put(END_PREFIX, END_PREFIX_COLUMN);
		propertyToColumnMap.put(PREFIX_LENGTH, PREFIX_LENGTH_COLUMN);
		propertyToColumnMap.put(PREFIX_MAX_LENGTH, PREFIX_MAX_LENGTH_COLUMN);
		propertyToColumnMap.put(TYPE, TYPE_COLUMN);
		propertyToColumnMap.put(COMMENT, COMMENT_COLUMN);
	}

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
		if (getAsn() != null) {
			statement.setLong(1, getAsn());
		} else {
			statement.setNull(1, Types.NUMERIC);
		}
		if (getPrefixText() != null) {
			statement.setString(2, getPrefixText());
		} else {
			statement.setNull(2, Types.VARCHAR);
		}
		if (getStartPrefix() != null) {
			statement.setBytes(3, getStartPrefix());
		} else {
			statement.setNull(3, Types.BLOB);
		}
		if (getEndPrefix() != null) {
			statement.setBytes(4, getEndPrefix());
		} else {
			statement.setNull(4, Types.BLOB);
		}
		if (getPrefixLength() != null) {
			statement.setInt(5, getPrefixLength());
		} else {
			statement.setNull(5, Types.INTEGER);
		}
		if (getPrefixMaxLength() != null) {
			statement.setInt(6, getPrefixMaxLength());
		} else {
			statement.setNull(6, Types.INTEGER);
		}
		if (getType() != null) {
			statement.setInt(7, getType());
		} else {
			statement.setNull(7, Types.INTEGER);
		}
		if (getComment() != null) {
			statement.setString(8, getComment());
		} else {
			statement.setNull(8, Types.VARCHAR);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		List<ValidationError> validationErrors = new ArrayList<>();
		if (operation == Operation.CREATE) {
			// Check the attributes according to the type
			// The ID isn't validated since this is a new object
			Integer type = this.getType();
			Long asn = this.getAsn();
			byte[] startPrefix = this.getStartPrefix();
			byte[] endPrefix = this.getEndPrefix();
			Integer prefixLength = this.getPrefixLength();
			Integer prefixMaxLength = this.getPrefixMaxLength();
			String comment = this.getComment();
			boolean keepValidating = true;
			if (type == null) {
				validationErrors.add(new ValidationError(OBJECT_NAME, TYPE, null, ValidationErrorType.NULL));
			} else if (type == TYPE_FILTER) {
				// Either an ASN or a Prefix must exist
				if (asn == null && startPrefix == null) {
					validationErrors.add(new ValidationError(OBJECT_NAME, ASN, null, ValidationErrorType.NULL));
					validationErrors
							.add(new ValidationError(OBJECT_NAME, START_PREFIX, null, ValidationErrorType.NULL));
				}
				// Max length is only for assertions
				if (prefixMaxLength != null) {
					validationErrors.add(new ValidationError(OBJECT_NAME, PREFIX_MAX_LENGTH, prefixMaxLength,
							ValidationErrorType.NOT_NULL));
				}
			} else if (type == TYPE_ASSERTION) {
				// Both ASN and a Prefix must exist
				if (asn == null) {
					validationErrors.add(new ValidationError(OBJECT_NAME, ASN, null, ValidationErrorType.NULL));
				}
				if (startPrefix == null) {
					validationErrors
							.add(new ValidationError(OBJECT_NAME, START_PREFIX, null, ValidationErrorType.NULL));
				}
			} else {
				validationErrors
						.add(new ValidationError(OBJECT_NAME, TYPE, type, ValidationErrorType.UNEXPECTED_VALUE));
			}
			if (asn != null) {
				if (asn < ApiObject.ASN_MIN_VALUE || asn > ApiObject.ASN_MAX_VALUE) {
					validationErrors.add(new ValidationError(OBJECT_NAME, ASN, asn,
							ValidationErrorType.VALUE_OUT_OF_RANGE, ApiObject.ASN_MIN_VALUE, ApiObject.ASN_MAX_VALUE));
				}
			}
			// "It is RECOMMENDED that an explanatory comment is also included" (RFC 8416)
			if (comment == null || comment.trim().isEmpty()) {
				validationErrors.add(new ValidationError(OBJECT_NAME, COMMENT, null, ValidationErrorType.NULL));
			} else if (!(comment.trim().length() >= COMMENT_MIN_LENGTH
					&& comment.trim().length() <= COMMENT_MAX_LENGTH)) {
				validationErrors.add(new ValidationError(OBJECT_NAME, COMMENT, comment,
						ValidationErrorType.LENGTH_OUT_OF_RANGE, COMMENT_MIN_LENGTH, COMMENT_MAX_LENGTH));
			}
			// Prefix and prefix length must be together
			if ((startPrefix != null && prefixLength == null) || (startPrefix == null && prefixLength != null)) {
				if (startPrefix == null) {
					validationErrors
							.add(new ValidationError(OBJECT_NAME, START_PREFIX, null, ValidationErrorType.NULL));
				}
				if (prefixLength == null) {
					validationErrors
							.add(new ValidationError(OBJECT_NAME, PREFIX_LENGTH, null, ValidationErrorType.NULL));
				}
			}
			if (prefixLength != null) {
				keepValidating = keepValidating
						&& validatePrefixLength(startPrefix, prefixLength, PREFIX_LENGTH, validationErrors);
				// If max length is indicated, it can't be grater than length
				if (prefixMaxLength != null && type == TYPE_ASSERTION && prefixMaxLength < prefixLength) {
					validationErrors.add(new ValidationError(OBJECT_NAME, PREFIX_MAX_LENGTH, prefixMaxLength,
							ValidationErrorType.VALUE_OUT_OF_RANGE, prefixLength, prefixLength));
					keepValidating = false;
				}
			}
			if (prefixMaxLength != null && type == TYPE_ASSERTION) {
				keepValidating = keepValidating
						&& validatePrefixLength(startPrefix, prefixMaxLength, PREFIX_MAX_LENGTH, validationErrors);
			}
			if (!keepValidating) {
				// There was an error that won't let the validations run normally
				throw new ValidationException(validationErrors);
			}
			// If there's a Start Prefix, then it must exist an End Prefix
			if (startPrefix != null) {
				validatePrefixValue(startPrefix, prefixLength, START_PREFIX, validationErrors);
				if (endPrefix == null) {
					validationErrors.add(new ValidationError(OBJECT_NAME, END_PREFIX, null, ValidationErrorType.NULL));
				} else {
					validatePrefixValue(endPrefix, prefixMaxLength == null ? prefixLength : prefixMaxLength, END_PREFIX,
							validationErrors);
				}
			} else {
				// Otherwise, the end prefix shouldn't exist
				if (endPrefix != null) {
					validationErrors
							.add(new ValidationError(OBJECT_NAME, END_PREFIX, endPrefix, ValidationErrorType.NOT_NULL));
				}
			}
		}
		if (!validationErrors.isEmpty()) {
			throw new ValidationException(validationErrors);
		}
	}

	/**
	 * Validates the prefix length based on the <code>prefix</code> and
	 * <code>prefixLength</code> received; if there's an error then add it to the
	 * <code>validationErrors</code> list using the <code>prefixLengthFieldId</code>
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param prefixLengthFieldId
	 * @param validationErrors
	 * @return <code>boolean</code> to indicate if there was an error
	 */
	private boolean validatePrefixLength(byte[] prefix, int prefixLength, String prefixLengthFieldId,
			List<ValidationError> validationErrors) {
		// Prefix length according to IP type
		int min = 0;
		int max = 0;
		try {
			InetAddress prefixAddress = InetAddress.getByAddress(prefix);
			max = prefixAddress instanceof Inet4Address ? 32 : 128;
		} catch (UnknownHostException e) {
			validationErrors
					.add(new ValidationError(OBJECT_NAME, START_PREFIX, prefix, ValidationErrorType.UNEXPECTED_TYPE));
			return false;
		}
		if (!(prefixLength >= min && prefixLength <= max)) {
			validationErrors.add(new ValidationError(OBJECT_NAME, prefixLengthFieldId, prefix,
					ValidationErrorType.VALUE_OUT_OF_RANGE, min, max));
			return false;
		}
		return true;
	}

	/**
	 * Validate the prefix value to assert that's actually an IP block, not any IP
	 * address, if there's an error then add it to the <code>validationErrors</code>
	 * list
	 * 
	 * @param prefix
	 * @param prefixLength
	 * @param prefixFieldId
	 * @param validationErrors
	 */
	private void validatePrefixValue(byte[] prefix, int prefixLength, String prefixFieldId,
			List<ValidationError> validationErrors) {
		int maxBytes = 0;
		try {
			InetAddress prefixAddress = InetAddress.getByAddress(prefix);
			maxBytes = prefixAddress instanceof Inet4Address ? 4 : 16;
		} catch (UnknownHostException e) {
			validationErrors
					.add(new ValidationError(OBJECT_NAME, prefixFieldId, prefix, ValidationErrorType.UNEXPECTED_TYPE));
			return;
		}
		int bytesBase = prefixLength / 8;
		int bitsBase = prefixLength % 8;
		byte[] prefixLengthMask = new byte[maxBytes];
		int currByte = 0;
		for (; currByte < bytesBase; currByte++) {
			prefixLengthMask[currByte] |= 255;
		}
		if (currByte < prefixLengthMask.length) {
			prefixLengthMask[currByte] = (byte) (255 << (8 - bitsBase));
		}
		BigInteger ip = new BigInteger(prefix);
		BigInteger mask = new BigInteger(prefixLengthMask);
		if (!ip.or(mask).equals(mask)) {
			validationErrors
					.add(new ValidationError(OBJECT_NAME, prefixFieldId, prefix, ValidationErrorType.UNEXPECTED_VALUE));
		}
	}
}
