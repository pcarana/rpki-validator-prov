package mx.nic.lab.rpki.sqlite.model;

import java.util.Properties;

/**
 * Responsible of load all the .sql files to its corresponding Model
 *
 */
public class QueryLoader {

	/**
	 * Read the required properties from the configuration and load all the models
	 * queries
	 * 
	 * @param config
	 */
	public static void init(Properties config) {
		// Optional property, use empty string value instead of null
		String schema = config.getProperty("schema", "").trim();
		loadModelsQueries(schema);
	}

	private static void loadModelsQueries(String schema) {
		GbrModel.loadQueryGroup(schema);
		RoaModel.loadQueryGroup(schema);
		RpkiObjectModel.loadQueryGroup(schema);
		RpkiRepositoryModel.loadQueryGroup(schema);
		SlurmBgpsecModel.loadQueryGroup(schema);
		SlurmPrefixModel.loadQueryGroup(schema);
		TalFileModel.loadQueryGroup(schema);
		TalModel.loadQueryGroup(schema);
		TalUriModel.loadQueryGroup(schema);
		ValidationCheckModel.loadQueryGroup(schema);
		ValidationRunModel.loadQueryGroup(schema);
	}
}
