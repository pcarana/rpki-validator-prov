package mx.nic.lab.rpki.sqlite.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.pojo.Gbr;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.sqlite.database.QueryGroup;
import mx.nic.lab.rpki.sqlite.object.GbrDbObject;

/**
 * Model to retrieve GBR data from the database
 *
 */
public class GbrModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(GbrModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "Gbr";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_BY_PARENT_CA = "getByParentCa";
	private static final String CREATE = "create";

	/**
	 * Loads the queries corresponding to this model, based on the QUERY_GROUP
	 * constant
	 * 
	 * @param schema
	 */
	public static void loadQueryGroup(String schema) {
		try {
			QueryGroup group = new QueryGroup(QUERY_GROUP, schema);
			setQueryGroup(group);
		} catch (IOException e) {
			throw new RuntimeException("Error loading query group", e);
		}
	}

	/**
	 * Get the {@link Class} to use as a lock
	 * 
	 * @return
	 */
	private static Class<GbrModel> getModelClass() {
		return GbrModel.class;
	}

	/**
	 * Get all the {@link Gbr}s related to a {@link Roa}, returns an empty list when
	 * no data is found
	 * 
	 * @param roa
	 * @param connection
	 * @return The list of {@link Gbr}s, or empty list when no data is found
	 * @throws SQLException
	 */
	public static List<Gbr> getByRoa(Roa roa, Connection connection) throws SQLException {
		List<Gbr> gbrs = new ArrayList<Gbr>();
		if (roa.getRpkiObject() == null) {
			// Nothing to do here
			return gbrs;
		}
		// Get the parent CA object
		RpkiObject parentCaObject = getParentCa(roa.getRpkiObject(), connection);
		if (parentCaObject == null) {
			return gbrs;
		}
		// Parent found, now get its GBR sons
		String query = getQueryGroup().getQuery(GET_BY_PARENT_CA);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, parentCaObject.getSubjectKeyIdentifier());
			statement.setString(2, RpkiObject.Type.GBR.toString());
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			while (rs.next()) {
				GbrDbObject gbr = new GbrDbObject(rs);
				gbr.setRpkiObject(RpkiObjectModel.getById(gbr.getRpkiObjectId(), connection));
				gbrs.add(gbr);
			}
			return gbrs;
		}
	}

	/**
	 * Creates a new {@link Gbr} returns null if the object couldn't be created.
	 * 
	 * @param newGbr
	 * @param connection
	 * @return The ID of the {@link Gbr} created
	 * @throws SQLException
	 */
	public static boolean create(Gbr newGbr, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(CREATE);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			GbrDbObject stored = new GbrDbObject(newGbr);
			stored.storeToDatabase(statement);
			int created = executeUpdate(statement, getModelClass(), logger);
			return created > 0;
		}
	}

	/**
	 * Get the parent CA of a {@link RpkiObject}, based on the AKI and SKI of the
	 * objects
	 * 
	 * @param rpkiObject
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	private static RpkiObject getParentCa(RpkiObject rpkiObject, Connection connection) throws SQLException {
		RpkiObject object = RpkiObjectModel.getBySubjectKeyIdentifier(rpkiObject.getAuthorityKeyIdentifier(),
				connection);
		if (object == null) {
			return null;
		}
		if (object.isCa()) {
			return object;
		}
		// Improbable, but still leave the case (dead end)
		if (object.getAuthorityKeyIdentifier() == null
				|| object.getAuthorityKeyIdentifier().equals(object.getSubjectKeyIdentifier())) {
			return null;
		}
		return getParentCa(object, connection);
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		GbrModel.queryGroup = queryGroup;
	}
}
