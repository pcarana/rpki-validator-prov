package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.primitives.UnsignedBytes;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.EncodedRpkiObject;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.db.pojo.RpkiObject.Type;
import mx.nic.lab.rpki.db.spi.RpkiObjectDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.RpkiObjectModel;
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
	public void bulkCreate(Set<RpkiObject> rpkiObjects) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			RpkiObjectModel.bulkCreate(rpkiObjects, connection);
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

	@Override
	public int updateReachedObjects(Set<RpkiObject> reachedObjects) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return RpkiObjectModel.updateReachedObjects(reachedObjects, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}
}
