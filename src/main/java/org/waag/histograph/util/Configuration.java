package org.waag.histograph.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.json.JSONObject;

public class Configuration {

	public String ELASTICSEARCH_HOST;
	public String ELASTICSEARCH_PORT;
	public String NEO4J_FILEPATH;
	public String NEO4J_PORT;
	public String REDIS_HOST;
	public String REDIS_PORT;
	public String REDIS_HISTOGRAPH_QUEUE;
	public String REDIS_ES_QUEUE;
	public String SCHEMA_DIR;
	
	public Configuration (String es_host, String es_port, String neo4j_filepath, String neo4j_port, String redis_host, 
									String redis_port, String redis_histograph_queue, String redis_es_queue, String schema_dir) {
		ELASTICSEARCH_HOST = es_host;
		ELASTICSEARCH_PORT = es_port;
		NEO4J_FILEPATH = neo4j_filepath;
		NEO4J_PORT = neo4j_port;
		REDIS_HOST = redis_host;
		REDIS_PORT = redis_port;
		REDIS_HISTOGRAPH_QUEUE = redis_histograph_queue;
		REDIS_ES_QUEUE = redis_es_queue;
		SCHEMA_DIR = schema_dir;
	}
	
	public static Configuration fromFile(String filePath) throws IOException {
		try {
			String contents = new String(Files.readAllBytes(Paths.get(filePath)));
			JSONObject configObj = new JSONObject(contents);
			return getConfigFromJSON(configObj);
		} catch (JSONException e) {
			throw new IOException("Invalid configuration file: " + e.getMessage());
		}
	}
	
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
			String es_host = configObj.getJSONObject("elasticsearch").getString("host");
			String es_port = configObj.getJSONObject("elasticsearch").get("port").toString();
			String neo4j_filepath = configObj.getJSONObject("neo4j").getString("path");
			String neo4j_port = configObj.getJSONObject("neo4j").get("port").toString();
			String redis_host = configObj.getJSONObject("redis").getString("host");
			String redis_port = configObj.getJSONObject("redis").get("port").toString();
			String redis_histograph_queue = configObj.getJSONObject("redis").getJSONObject("queues").getString("core");
			String redis_es_queue = configObj.getJSONObject("redis").getJSONObject("queues").getString("elasticsearch");
			String schema_dir = configObj.getJSONObject("schemas").getString("dir");
			
			return new Configuration(es_host, es_port, neo4j_filepath, neo4j_port, redis_host, redis_port, redis_histograph_queue, redis_es_queue, schema_dir);
		} catch (JSONException e) {
			throw new IOException("Invalid configuration file: " + e.getMessage());
		}
	}
}
