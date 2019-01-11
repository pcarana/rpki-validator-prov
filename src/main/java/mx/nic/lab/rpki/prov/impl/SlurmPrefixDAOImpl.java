package mx.nic.lab.rpki.prov.impl;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.db.spi.SlurmPrefixDAO;
import mx.nic.lab.rpki.prov.database.DatabaseSession;
import mx.nic.lab.rpki.prov.model.SlurmPrefixModel;
import mx.nic.lab.rpki.prov.object.SlurmPrefixDbObject;
import mx.nic.lab.rpki.prov.object.DatabaseObject.Operation;

/**
 * Implementation to retrieve SLURM Prefix data
 *
 */
public class SlurmPrefixDAOImpl implements SlurmPrefixDAO {

	@Override
	public SlurmPrefix getById(Long id) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getById(id, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public ListResult<SlurmPrefix> getAll(PagingParameters pagingParams) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getAll(pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public ListResult<SlurmPrefix> getAllByType(String type, PagingParameters pagingParams)
			throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getAllByType(type, pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean create(SlurmPrefix newSlurmPrefix) throws ApiDataAccessException {
		byte[] endPrefix = Roa.calculateEndPrefix(newSlurmPrefix.getStartPrefix(), newSlurmPrefix.getPrefixLength(),
				newSlurmPrefix.getPrefixMaxLength());
		if (newSlurmPrefix.getType() != null && newSlurmPrefix.getType().equals(SlurmPrefix.TYPE_FILTER)) {
			endPrefix = calculateFilterEndPrefix(endPrefix, newSlurmPrefix);
		}
		newSlurmPrefix.setEndPrefix(endPrefix);
		SlurmPrefixDbObject slurmPrefixDb = new SlurmPrefixDbObject(newSlurmPrefix);
		slurmPrefixDb.validate(Operation.CREATE);

		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object doesn't exists
			if (SlurmPrefixModel.exist(newSlurmPrefix, connection)) {
				throw new ValidationException(
						new ValidationError(SlurmPrefix.OBJECT_NAME, ValidationErrorType.OBJECT_EXISTS));
			}
			return SlurmPrefixModel.create(newSlurmPrefix, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean deleteById(Long id) throws ApiDataAccessException {
		// First check that the object actually exists
		try (Connection connection = DatabaseSession.getConnection()) {
			SlurmPrefix prefix = SlurmPrefixModel.getById(id, connection);
			if (prefix == null) {
				throw new ValidationException(
						new ValidationError(SlurmPrefix.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			int deleted = SlurmPrefixModel.deleteById(id, connection);
			return deleted > 0;
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public SlurmPrefix getPrefixByProperties(Long asn, byte[] prefix, Integer prefixLength, Integer maxPrefixLength,
			String type) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getByProperties(asn, prefix, prefixLength, maxPrefixLength, type, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public int updateComment(Long id, String newComment) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object exists
			if (SlurmPrefixModel.getById(id, connection) == null) {
				throw new ValidationException(
						new ValidationError(SlurmPrefix.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			return SlurmPrefixModel.updateComment(id, newComment, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public int updateOrder(Long id, int newOrder) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			// Validate that the object exists
			if (SlurmPrefixModel.getById(id, connection) == null) {
				throw new ValidationException(
						new ValidationError(SlurmPrefix.OBJECT_NAME, ValidationErrorType.OBJECT_NOT_EXISTS));
			}
			return SlurmPrefixModel.updateOrder(id, newOrder, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public void bulkDelete(Set<Long> ids) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			SlurmPrefixModel.bulkDelete(ids, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	/**
	 * Calculate the end of a prefix filter, in this case the end prefix will be the
	 * last valid IP of the IP block
	 * 
	 * @param currentEndPrefix
	 * @param slurmPrefix
	 * @return
	 */
	private byte[] calculateFilterEndPrefix(byte[] currentEndPrefix, SlurmPrefix slurmPrefix) {
		if (currentEndPrefix == null || slurmPrefix.getStartPrefix() == null || slurmPrefix.getPrefixLength() == null) {
			return currentEndPrefix;
		}
		int bytesBase = slurmPrefix.getPrefixLength() / 8;
		int bitsBase = slurmPrefix.getPrefixLength() % 8;
		byte[] prefixLengthMask = new byte[currentEndPrefix.length];
		int currByte = bytesBase;
		if (bitsBase > 0) {
			prefixLengthMask[currByte++] |= ((byte) (255 >> bitsBase));
		}
		for (; currByte < prefixLengthMask.length; currByte++) {
			prefixLengthMask[currByte] |= 255;
		}
		BigInteger endIp = new BigInteger(currentEndPrefix);
		BigInteger mask = new BigInteger(prefixLengthMask);
		return endIp.or(mask).toByteArray();
	}
}
