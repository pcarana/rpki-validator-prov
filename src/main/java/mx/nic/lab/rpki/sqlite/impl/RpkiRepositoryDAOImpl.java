package mx.nic.lab.rpki.sqlite.impl;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import mx.nic.lab.rpki.db.exception.ApiDataAccessException;
import mx.nic.lab.rpki.db.pojo.PagingParameters;
import mx.nic.lab.rpki.db.pojo.RpkiRepository;
import mx.nic.lab.rpki.db.spi.RpkiRepositoryDAO;
import mx.nic.lab.rpki.sqlite.database.DatabaseSession;
import mx.nic.lab.rpki.sqlite.model.RpkiRepositoryModel;

/**
 * Implementation to retrieve the RPKI Repositories
 *
 */
public class RpkiRepositoryDAOImpl implements RpkiRepositoryDAO {

	@Override
	public Optional<RpkiRepository> findByURI(String uri) throws ApiDataAccessException {
		String normalized = URI.create(uri).normalize().toASCIIString();
		try (Connection connection = DatabaseSession.getConnection()) {
			return Optional.ofNullable(RpkiRepositoryModel.getByUri(normalized, connection));
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

	@Override
	public Stream<RpkiRepository> findRsyncRepositories() throws ApiDataAccessException {
		LinkedHashMap<String, String> sortMap = new LinkedHashMap<>();
		sortMap.put(RpkiRepository.LOCATION_URI, PagingParameters.ORDER_ASC);
		sortMap.put(RpkiRepository.ID, PagingParameters.ORDER_ASC);
		PagingParameters pagingParameters = new PagingParameters();
		pagingParameters.setSort(sortMap);
		try (Connection connection = DatabaseSession.getConnection()) {
			List<RpkiRepository> repositories = RpkiRepositoryModel.getAll(pagingParameters, connection);
			return repositories.stream();
		} catch (SQLException e) {
			throw new ApiDataAccessException(e);
		}
	}

}
