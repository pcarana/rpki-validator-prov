package mx.nic.lab.rpki.sqlite.object;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.RpkiRepository;

public class RpkiRepositoryDbObject extends RpkiRepository implements DatabaseObject {

	public static final String ID_COLUMN = "rpr_id";
	public static final String UPDATED_AT_COLUMN = "rpr_updated_at";
	public static final String STATUS_COLUMN = "rpr_status";
	public static final String LAST_DOWNLOADED_AT_COLUMN = "rpr_last_downloaded_at";
	public static final String LOCATION_URI_COLUMN = "rpr_location_uri";
	public static final String PARENT_REPOSITORY_COLUMN = "rpr_parent_repository_id";

	/**
	 * Mapping of the {@link RpkiRepository} properties to its corresponding DB
	 * column
	 */
	public static final Map<String, String> propertyToColumnMap;
	static {
		propertyToColumnMap = new HashMap<>();
		propertyToColumnMap.put(ID, ID_COLUMN);
		propertyToColumnMap.put(UPDATED_AT, UPDATED_AT_COLUMN);
		propertyToColumnMap.put(STATUS, STATUS_COLUMN);
		propertyToColumnMap.put(LAST_DOWNLOADED_AT, LAST_DOWNLOADED_AT_COLUMN);
		propertyToColumnMap.put(LOCATION_URI, LOCATION_URI_COLUMN);
		propertyToColumnMap.put(PARENT_REPOSITORY, PARENT_REPOSITORY_COLUMN);
	}

	private Long parentRepositoryId;

	public RpkiRepositoryDbObject() {
		super();
	}

	public RpkiRepositoryDbObject(RpkiRepository rpkiRepository) {
		this.setId(rpkiRepository.getId());
		this.setUpdatedAt(rpkiRepository.getUpdatedAt());
		this.setStatus(rpkiRepository.getStatus());
		this.setLastDownloadedAt(rpkiRepository.getLastDownloadedAt());
		this.setLocationUri(rpkiRepository.getLocationUri());
		this.setParentRepository(rpkiRepository.getParentRepository());
		this.setTrustAnchors(rpkiRepository.getTrustAnchors());
		if (getParentRepository() != null) {
			this.setParentRepositoryId(getParentRepository().getId());
		}
	}

	public RpkiRepositoryDbObject(ResultSet resultSet) throws SQLException {
		super();
		loadFromDatabase(resultSet);
	}

	@Override
	public void loadFromDatabase(ResultSet resultSet) throws SQLException {
		setId(resultSet.getLong(ID_COLUMN));
		if (resultSet.wasNull()) {
			setId(null);
		}
		setUpdatedAt(getStringDateAsInstant(resultSet.getString(UPDATED_AT_COLUMN)));
		setStatus(getStringAsEnum(Status.class, resultSet.getString(STATUS_COLUMN)));
		setLastDownloadedAt(getStringDateAsInstant(resultSet.getString(LAST_DOWNLOADED_AT_COLUMN)));
		setLocationUri(resultSet.getString(LOCATION_URI_COLUMN));
		setParentRepositoryId(resultSet.getLong(PARENT_REPOSITORY_COLUMN));
		if (resultSet.wasNull()) {
			setParentRepositoryId(null);
		}
	}

	@Override
	public void storeToDatabase(PreparedStatement statement) throws SQLException {
		statement.setLong(1, getId());
		if (getUpdatedAt() != null) {
			statement.setString(2, getUpdatedAt().toString());
		} else {
			statement.setNull(2, Types.VARCHAR);
		}
		if (getStatus() != null) {
			statement.setString(3, getStatus().toString());
		} else {
			statement.setNull(3, Types.VARCHAR);
		}
		if (getLastDownloadedAt() != null) {
			statement.setString(4, getLastDownloadedAt().toString());
		} else {
			statement.setNull(4, Types.VARCHAR);
		}
		if (getLocationUri() != null) {
			statement.setString(5, getLocationUri());
		} else {
			statement.setNull(5, Types.VARCHAR);
		}
		if (getParentRepositoryId() != null) {
			statement.setLong(6, getParentRepositoryId());
		} else {
			statement.setNull(6, Types.NUMERIC);
		}
	}

	@Override
	public void validate(Operation operation) throws ValidationException {
		List<ValidationError> validationErrors = new ArrayList<>();
		if (operation == Operation.CREATE) {

		}
		if (!validationErrors.isEmpty()) {
			throw new ValidationException(validationErrors);
		}
	}

	public Long getParentRepositoryId() {
		return parentRepositoryId;
	}

	public void setParentRepositoryId(Long parentRepositoryId) {
		this.parentRepositoryId = parentRepositoryId;
	}
}
