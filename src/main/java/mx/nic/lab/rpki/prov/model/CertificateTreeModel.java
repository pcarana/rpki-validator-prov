package mx.nic.lab.rpki.prov.model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import mx.nic.lab.rpki.db.cert.tree.CertificateNode;
import mx.nic.lab.rpki.db.cert.tree.CertificationTreeNode;
import mx.nic.lab.rpki.db.cert.tree.GbrNode;
import mx.nic.lab.rpki.db.cert.tree.ResourceNode;
import mx.nic.lab.rpki.db.cert.tree.RoaNode;
import mx.nic.lab.rpki.db.pojo.Gbr;
import mx.nic.lab.rpki.db.pojo.Roa;
import mx.nic.lab.rpki.db.pojo.RpkiObject;
import mx.nic.lab.rpki.db.pojo.RpkiObject.Type;
import mx.nic.lab.rpki.prov.database.QueryGroup;
import mx.nic.lab.rpki.prov.object.RpkiObjectDbObject;
import mx.nic.lab.rpki.db.pojo.Tal;

/**
 * Model to retrieve RPKI Objects as Certificate Trees from the database
 *
 */
public class CertificateTreeModel extends DatabaseModel {

	private static final Logger logger = Logger.getLogger(CertificateTreeModel.class.getName());

	/**
	 * Query group ID, it MUST be the same that the .sql file where the queries are
	 * found
	 */
	private static final String QUERY_GROUP = "CertificateTree";

	private static QueryGroup queryGroup = null;

	// Queries IDs used by this model
	private static final String GET_FROM_ROOT = "geFromRoot";
	private static final String COUNT_CHILDS = "countChilds";

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
	private static Class<CertificateTreeModel> getModelClass() {
		return CertificateTreeModel.class;
	}

	/**
	 * Get the {@link CertificationTreeNode}s root, where the parent node is the
	 * {@link Tal}
	 * 
	 * @param tal
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static CertificationTreeNode findFromRoot(Tal tal, Connection connection) throws SQLException {
		// Start setting the root node
		CertificateNode root = new CertificateNode();
		root.setId(tal.getId());
		tal.getTalUris().forEach((talUri) -> {
			root.getLocations().add(talUri.getLocation());
		});
		root.setSubjectKeyIdentifier(tal.getCertificate().getSubjectKeyIdentifier());

		// And get the rest of the data from the DB (loads only the direct childs, isn't
		// recursive)
		loadChilds(root, connection);
		return root;
	}

	/**
	 * Get the {@link CertificationTreeNode}s, where the parent node is a
	 * Certificate distinct than the root (TALs certificate)
	 * 
	 * @param certId
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	public static CertificationTreeNode findFromChild(Long certId, Connection connection) throws SQLException {
		// Must exist, and must be a certificate
		RpkiObject rootObject = RpkiObjectModel.getById(certId, connection);
		if (rootObject == null
				|| !(rootObject.getType().equals(Type.CER) || rootObject.getType().equals(Type.ROUTER_CER))) {
			return null;
		}
		CertificateNode root = new CertificateNode();
		root.setId(rootObject.getId());
		root.setLocations(new ArrayList<>(rootObject.getLocations()));
		root.setSubjectKeyIdentifier(rootObject.getSubjectKeyIdentifier());

		// And get the rest of the data from the DB (loads only the direct childs, isn't
		// recursive)
		loadChilds(root, connection);
		return root;
	}

	/**
	 * Load the childs related to the root parent
	 * 
	 * @param root
	 * @param connection
	 * @throws SQLException
	 */
	private static void loadChilds(CertificateNode root, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(GET_FROM_ROOT);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, root.getSubjectKeyIdentifier());
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			while (rs.next()) {
				CertificationTreeNode child = null;
				Type type = Type.valueOf(rs.getString(RpkiObjectDbObject.TYPE_COLUMN));
				Long objectId = rs.getLong(RpkiObjectDbObject.ID_COLUMN);
				byte[] subjectKeyIdentifier = rs.getBytes(RpkiObjectDbObject.SUBJECT_KEY_IDENTIFIER_COLUMN);
				switch (type) {
				case ROUTER_CER:
				case CER:
					CertificateNode tempCer = new CertificateNode();
					tempCer.setId(objectId);
					tempCer.setChildCount(getChildCount(subjectKeyIdentifier, connection));
					child = tempCer;
					break;
				case ROA:
					RoaNode tempRoa = new RoaNode();
					List<Roa> roas = RoaModel.getByRpkiObjectId(objectId, connection);
					roas.forEach((roa) -> {
						tempRoa.getResources().add(new ResourceNode(roa.getAsn(), roa.getPrefixText(),
								roa.getPrefixMaxLength(), roa.getId()));
					});
					child = tempRoa;
					break;
				case GBR:
					GbrNode tempGbr = new GbrNode();
					Gbr gbr = GbrModel.getByRpkiObjectId(objectId, connection);
					if (gbr != null) {
						tempGbr.setVCard(gbr.getVcard());
					}
					child = tempGbr;
					break;
				default:
					// Default case for MFT, CRL and OTHER
					child = new CertificationTreeNode();
					child.setType(type);
					break;
				}
				child.setLocations(new ArrayList<>(RpkiObjectModel.getLocations(objectId, connection)));
				child.setSubjectKeyIdentifier(subjectKeyIdentifier);
				root.getChilds().add(child);
			}
			root.setChildCount(root.getChilds().size());
		}
	}

	/**
	 * Get the count of all the childs of the SKI received
	 * 
	 * @param subjectKeyIdentifier
	 * @param connection
	 * @return The count of all the SKI childs
	 * @throws SQLException
	 */
	private static Integer getChildCount(byte[] subjectKeyIdentifier, Connection connection) throws SQLException {
		String query = getQueryGroup().getQuery(COUNT_CHILDS);
		try (PreparedStatement statement = prepareStatement(connection, query, getModelClass())) {
			statement.setBytes(1, subjectKeyIdentifier);
			ResultSet rs = executeQuery(statement, getModelClass(), logger);
			if (rs.next()) {
				return rs.getInt(1);
			}
			return 0;
		}
	}

	public static QueryGroup getQueryGroup() {
		return queryGroup;
	}

	public static void setQueryGroup(QueryGroup queryGroup) {
		CertificateTreeModel.queryGroup = queryGroup;
	}
}
