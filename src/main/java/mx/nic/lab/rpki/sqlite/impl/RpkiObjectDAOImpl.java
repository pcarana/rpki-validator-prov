package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.primitives.UnsignedBytes;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.EncodedRpkiObject;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.db.pojo.RpkiObject.Type;
import mx.nic.lab.rpki.db.spi.RpkiObjectDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.RpkiObjectModel;
import mx.nic.lab.rpki.sqlite.object.DatabaseObject.Operation;
import mx.nic.lab.rpki.sqlite.object.RpkiObjectDbObject;
import net.ripe.rpki.commons.crypto.CertificateRepositoryObject;
import net.ripe.rpki.commons.crypto.cms.manifest.ManifestCms;
import net.ripe.rpki.commons.validation.ValidationResult;

/**
 * Implementation to retrieve the RPKI Objects
 *
 */
public class RpkiObjectDAOImpl implements RpkiObjectDAO {

	@Override
	public RpkiObject getById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RpkiObjectModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Long create(RpkiObject rpkiObject) throws ApiDataAccessException {
		RpkiObjectDbObject rpkiObjectDb = new RpkiObjectDbObject(rpkiObject);
		rpkiObjectDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object doesn't exists
			if (RpkiObjectModel.exist(rpkiObject, connection)) {
				throw new ValidationException(
						new ValidationError(RpkiObject.OBJECT_NAME, ValidationErrorType.OBJECT_EXISTS));
			}
			return RpkiObjectModel.create(rpkiObject, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean delete(RpkiObject rpkiObject) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object exists
			if (!RpkiObjectModel.exist(rpkiObject, connection)) {
				throw new ValidationException(
						new ValidationError(RpkiObject.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			int deleted = RpkiObjectModel.delete(rpkiObject, connection);
			return deleted > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public <T extends CertificateRepositoryObject> Optional<T> findCertificateRepositoryObject(long rpkiObjectId,
			Class<T> clazz, ValidationResult validationResult) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			EncodedRpkiObject encodedObject = RpkiObjectModel.getEncodedByRpkiObjectId(rpkiObjectId, connection);
			if (encodedObject == null) {
				return null;
			}
			return encodedObject.get(clazz, validationResult);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Optional<RpkiObject> findBySha256(byte[] sha256) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return Optional.ofNullable(RpkiObjectModel.getBySha256(sha256, connection));
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Map<String, RpkiObject> findObjectsInManifest(ManifestCms manifestCms) throws ApiDataAccessException {
		SortedMap<byte[], String> hashes = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
		manifestCms.getFiles().forEach((name, hash) -> hashes.put(hash, name));
		List<RpkiObject> rpkiObjects = null;
		try (Connection connection = DatabaseSession.getConnection()) {
			rpkiObjects = RpkiObjectModel.getBySha256Set(hashes.keySet(), connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
		return rpkiObjects.stream().collect(Collectors.toMap(x -> hashes.get(x.getSha256()), x -> x));
	}

	@Override
	public Optional<RpkiObject> findLatestByTypeAndAuthorityKeyIdentifier(Type type, byte[] authorityKeyIdentifier)
			throws ApiDataAccessException {
		LinkedHashMap<String, String> sortMap = new LinkedHashMap<>();
		sortMap.put(RpkiObject.SERIAL_NUMBER, PagingParameters.ORDER_DESC);
		sortMap.put(RpkiObject.SIGNING_TIME, PagingParameters.ORDER_DESC);
		sortMap.put(RpkiObject.ID, PagingParameters.ORDER_DESC);
		PagingParameters pagingParameters = new PagingParameters();
		pagingParameters.setSort(sortMap);
		try (Connection connection = DatabaseSession.getConnection()) {
			return Optional.ofNullable(RpkiObjectModel.getLatestByTypeAndAuthorityKeyIdentifier(type,
					authorityKeyIdentifier, pagingParameters, connection));
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public long deleteUnreachableObjects(Instant unreachableSince) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RpkiObjectModel.deleteUnreachableObjects(unreachableSince, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean addRpkiRepository(RpkiObject rpkiObject, Long rpkiRepositoryId) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			int added = RpkiObjectModel.addRpkiRepository(rpkiObject.getId(), rpkiRepositoryId, connection);
			return added > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
