package mx.nic.lab.rpki.sqlite.impl;

import java.sql.Connection;
import java.sql.SQLException;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.exception.ValidationError;
import mx.nic.lab.rpki.db.exception.ValidationErrorType;
import mx.nic.lab.rpki.db.exception.ValidationException;
import mx.nic.lab.rpki.db.pojo.ListResult;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.SlurmPrefix;
import mx.nic.lab.rpki.db.spi.SlurmPrefixDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.SlurmPrefixModel;
import mx.nic.lab.rpki.sqlite.object.DatabaseObject.Operation;
import mx.nic.lab.rpki.sqlite.object.SlurmPrefixDbObject;

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
	public ListResult<SlurmPrefix> getAllByType(String type, PagingParameters pagingParams) throws ApiDataAccessException {
		try (Connection connection = DatabaseSession.getConnection()) {
			return SlurmPrefixModel.getAllByType(type, pagingParams, connection);
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public boolean create(SlurmPrefix newSlurmPrefix) throws ApiDataAccessException {
		newSlurmPrefix.setEndPrefix(calculateEndPrefix(newSlurmPrefix));
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

	/**
	 * Calculates the End Prefix based on the prefix length and max prefix length
	 * 
	 * @param slurmPrefix
	 * @return
	 */
	private byte[] calculateEndPrefix(SlurmPrefix slurmPrefix) {
		byte[] startPrefix = slurmPrefix.getStartPrefix();
		Integer prefixLength = slurmPrefix.getPrefixLength();
		Integer maxPrefixLength = slurmPrefix.getPrefixMaxLength();
		// If applies, only when there's a Prefix Max Length and is in a valid range
		if (startPrefix == null || prefixLength == null || maxPrefixLength == null || maxPrefixLength <= prefixLength
				|| maxPrefixLength > startPrefix.length * 8) {
			return startPrefix;
		}
		byte[] endPrefix = startPrefix.clone();
		int bytesBase = prefixLength / 8;
		int bitsBase = prefixLength % 8;
		int bytesMask = maxPrefixLength / 8;
		int bitsMask = maxPrefixLength % 8;
		if (maxPrefixLength > prefixLength && bytesBase < endPrefix.length) {
			int currByte = bytesBase;
			if (bytesMask > bytesBase) {
				endPrefix[currByte] |= (255 >> bitsBase);
				currByte++;
				for (; currByte < bytesMask; currByte++) {
					endPrefix[currByte] |= 255;
				}
				bitsBase = 0;
			}
			if (currByte < endPrefix.length) {
				endPrefix[currByte] |= ((byte) (255 << (8 - bitsMask)) & (255 >> bitsBase));
			}
		}
		return endPrefix;
	}
}
