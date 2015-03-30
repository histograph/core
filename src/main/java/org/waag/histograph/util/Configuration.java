package org.waag.histograph.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A class that parses the Histograph configuration JSON file and contains all configuration parameters.
 * The Configuration object can only be created with the {@link #fromEnv()} and {@link #fromFile(String)} methods.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class Configuration {

	public String ELASTICSEARCH_HOST;
	public String ELASTICSEARCH_PORT;
	public String ELASTICSEARCH_INDEX;
	public String ELASTICSEARCH_TYPE;
	
	public String PG_HOST;
	public String PG_PORT;
	public String PG_USER;
	public String PG_PASS;
	public String PG_DB;
	
	public String NEO4J_FILEPATH;
	public String NEO4J_PORT;
	
	public String REDIS_HOST;
	public String REDIS_PORT;
	
	public String REDIS_MAIN_QUEUE;
	public String REDIS_GRAPH_QUEUE;
	public String REDIS_ES_QUEUE;
	public String REDIS_PG_QUEUE;
	
	public int TRAVERSAL_PORT;
	
	public String SCHEMA_DIR;
	
	private Configuration (String es_host, String es_port, String es_index, String es_type,
							String pg_host, String pg_port, String pg_user, String pg_pass, String pg_db,
							String neo4j_filepath, String neo4j_port, int traversal_port,
							String redis_host, String redis_port, String redis_main_queue, 
							String redis_graph_queue, String redis_es_queue, String redis_pg_queue, String schema_dir) {
		ELASTICSEARCH_HOST = es_host;
		ELASTICSEARCH_PORT = es_port;
		ELASTICSEARCH_INDEX = es_index;
		ELASTICSEARCH_TYPE = es_type;
		
		PG_HOST = pg_host;
		PG_PORT = pg_port;
		PG_USER = pg_user;
		PG_PASS = pg_pass;
		PG_DB = pg_db;
		
		NEO4J_FILEPATH = neo4j_filepath;
		NEO4J_PORT = neo4j_port;
		TRAVERSAL_PORT = traversal_port;
		
		REDIS_HOST = redis_host;
		REDIS_PORT = redis_port;
		REDIS_MAIN_QUEUE = redis_main_queue;
		REDIS_GRAPH_QUEUE = redis_graph_queue;
		REDIS_ES_QUEUE = redis_es_queue;
		REDIS_PG_QUEUE = redis_pg_queue;
		
		SCHEMA_DIR = schema_dir;
	}
	
	/**
	 * Creates a Configuration object from a file.
	 * @param filePath The path of the JSON configuration file.
	 * @return A Configuration object in which all configuration parameters are set.
	 * @throws IOException Thrown if an invalid configuration file is found.
	 */
	public static Configuration fromFile(String filePath) throws IOException {
		try {
			String contents = new String(Files.readAllBytes(Paths.get(filePath)));
			JSONObject configObj = new JSONObject(contents);
			return getConfigFromJSON(configObj);
		} catch (JSONException e) {
			throw new IOException("Invalid configuration file: " + e.getMessage());
		}
	}
	
	/**
	 * Creates a Configuration object based on a file path found in the <i>HISTOGRAPH_CONFIG</i> environment variable.
	 * @return A Configuration object in which all configuration parameters are set.
	 * @throws IOException Thrown if an invalid configuration file is provided, or if the environment variable was not set.
	 */
	public static Configuration fromEnv() throws IOException {
		try {
			String filePath = System.getenv("HISTOGRAPH_CONFIG");
			String contents = new String(Files.readAllBytes(Paths.get(filePath)));
			JSONObject configObj = new JSONObject(contents);
			return getConfigFromJSON(configObj);
		} catch (NullPointerException e) {
			throw new IOException("Environment variable HISTOGRAPH_CONFIG not set. Set it or provide config file with arguments '-config <file>'");
		}
	}
	
	private static Configuration getConfigFromJSON(JSONObject configObj) throws IOException {
		try {
			String es_host = configObj.getJSONObject("elasticsearch").get("host").toString();
			String es_port = configObj.getJSONObject("elasticsearch").get("port").toString();
			String es_index = configObj.getJSONObject("elasticsearch").get("index").toString();
			String es_type = configObj.getJSONObject("elasticsearch").get("type").toString();
			
			String pg_host = configObj.getJSONObject("core").getJSONObject("postgresql").get("host").toString();
			String pg_port = configObj.getJSONObject("core").getJSONObject("postgresql").get("port").toString();
			String pg_user = configObj.getJSONObject("core").getJSONObject("postgresql").get("user").toString();
			String pg_pass = configObj.getJSONObject("core").getJSONObject("postgresql").get("pass").toString();
			String pg_db = configObj.getJSONObject("core").getJSONObject("postgresql").get("db").toString();
			
			String neo4j_filepath = configObj.getJSONObject("core").getJSONObject("neo4j").get("path").toString();
			String neo4j_port = configObj.getJSONObject("core").getJSONObject("neo4j").get("port").toString();
			int traversal_port = configObj.getJSONObject("core").getJSONObject("traversal").getInt("port");
			
			String redis_host = configObj.getJSONObject("redis").get("host").toString();
			String redis_port = configObj.getJSONObject("redis").get("port").toString();
			String redis_main_queue = configObj.getJSONObject("redis").getJSONObject("queues").get("histograph").toString();
			String redis_graph_queue = configObj.getJSONObject("redis").getJSONObject("queues").get("graph").toString();
			String redis_es_queue = configObj.getJSONObject("redis").getJSONObject("queues").get("es").toString();
			String redis_pg_queue = configObj.getJSONObject("redis").getJSONObject("queues").get("pg").toString();
			
			String schema_dir = configObj.getJSONObject("schemas").get("dir").toString();
			
			return new Configuration(es_host, es_port, es_index, es_type,
					pg_host, pg_port, pg_user, pg_pass, pg_db,
					neo4j_filepath, neo4j_port, traversal_port, 
					redis_host, redis_port, redis_main_queue, 
					redis_graph_queue, redis_es_queue, redis_pg_queue, schema_dir);
		} catch (JSONException e) {
			throw new IOException("Invalid configuration file: " + e.getMessage());
		}
	}
}