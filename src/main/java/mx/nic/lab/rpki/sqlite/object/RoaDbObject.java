package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.Roa;

/**
 * Extension of {@link Roa} as a {@link DatabaseObject}
 *
 */
public class RoaDbObject extends Roa implements DatabaseObject {

	public static final String RPKI_OBJECT_COLUMN = "rpo_id";
	public static final String ID_COLUMN = "roa_id";
	public static final String ASN_COLUMN = "roa_asn";
	public static final String PREFIX_TEXT_COLUMN = "roa_prefix_text";
	public static final String START_PREFIX_COLUMN = "roa_start_prefix";
	public static final String END_PREFIX_COLUMN = "roa_end_prefix";
	public static final String PREFIX_LENGTH_COLUMN = "roa_prefix_length";
	public static final String PREFIX_MAX_LENGTH_COLUMN = "roa_prefix_max_length";
	public static final String PREFIX_FAMILY_COLUMN = "roa_prefix_family";

	private Long rpkiObjectId;

	/**
	 * Mapping of the {@link Roa} properties to its corresponding DB column
	 */
	public static final Map<String, String> propertyToColumnMap;
	static {
		propertyToColumnMap = new HashMap<>();
		propertyToColumnMap.put(RPKI_OBJECT, RPKI_OBJECT_COLUMN);
		propertyToColumnMap.put(ID, ID_COLUMN);
		propertyToColumnMap.put(ASN, ASN_COLUMN);
		propertyToColumnMap.put(PREFIX_TEXT, PREFIX_TEXT_COLUMN);
		propertyToColumnMap.put(START_PREFIX, START_PREFIX_COLUMN);
		propertyToColumnMap.put(END_PREFIX, END_PREFIX_COLUMN);
		propertyToColumnMap.put(PREFIX_LENGTH, PREFIX_LENGTH_COLUMN);
		propertyToColumnMap.put(PREFIX_MAX_LENGTH, PREFIX_MAX_LENGTH_COLUMN);
		propertyToColumnMap.put(PREFIX_FAMILY, PREFIX_FAMILY_COLUMN);
	}

	public RoaDbObject() {
		super();
	}

	public RoaDbObject(Roa roa) {
		this.setRpkiObject(roa.getRpkiObject());
		this.setId(roa.getId());
		this.setAsn(roa.getAsn());
		this.setPrefixText(roa.getPrefixText());
		this.setStartPrefix(roa.getStartPrefix());
		this.setEndPrefix(roa.getEndPrefix());
		this.setPrefixLength(roa.getPrefixLength());
		this.setPrefixMaxLength(roa.getPrefixMaxLength());
		this.setPrefixFamily(roa.getPrefixFamily());
		if (getRpkiObject() != null) {
			this.setRpkiObjectId(getRpkiObject().getId());
		}
	}

	/**
	 * Create a new instance loading values from a <code>ResultSet</code>
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	public RoaDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setRpkiObjectId(resultSet.getLong(RPKI_OBJECT_COLUMN));
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
		setPrefixFamily(resultSet.getInt(PREFIX_FAMILY_COLUMN));
		if (resultSet.wasNull()) {
			setPrefixFamily(null);
		}
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		statement.setLong(1, getRpkiObjectId());
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
		if (getPrefixFamily() != null) {
			statement.setInt(8, getPrefixFamily());
		} else {
			statement.setNull(8, Types.INTEGER);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		// No special validations for now
	}

	public Long getRpkiObjectId() {
		return rpkiObjectId;
	}

	public void setRpkiObjectId(Long rpkiObjectId) {
		this.rpkiObjectId = rpkiObjectId;
	}
}
