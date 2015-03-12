package org.waag.histograph.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.Status;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.json.JSONObject;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;

public class ESMethods {
	
	public static void testConnection (JestClient client, Configuration config) {
		try {
			client.execute(new Status.Builder().build());
		} catch (Exception e) {
			System.out.println("Error: Could not connect to Elasticsearch at http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT);
			System.exit(1);
		}
	}
	
	public static void createIndex (JestClient client, Configuration config) {		
		try {
			client.execute(new CreateIndex.Builder(config.ELASTICSEARCH_INDEX).build());
		} catch (Exception e) {
			System.out.println("Error: Unable to create index. " + e.getMessage());
			System.exit(1);		}
		
		String mappingFilePath = config.SCHEMA_DIR + "/elasticsearch/pit.json";

		try {
			String mapping = new String(Files.readAllBytes(Paths.get(mappingFilePath)));
			PutMapping putMapping = new PutMapping.Builder(config.ELASTICSEARCH_INDEX, config.ELASTICSEARCH_TYPE, mapping).build();
			client.execute(putMapping);
		} catch (IOException e) {
			System.out.println("Error: Unable to read mapping file. " + e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Error: Unable to put mapping. " + e.getMessage());
			System.exit(1);
		}
	}
	
	public static boolean indexExists (JestClient client, Configuration config) {
		JestResult result;
		try {
			result = client.execute(new GetMapping.Builder().addIndex(config.ELASTICSEARCH_INDEX).addType(config.ELASTICSEARCH_TYPE).build());
			JSONObject obj = new JSONObject(result.getJsonString());

			if (obj.has("error") && obj.has("status") && obj.getInt("status") == 404) {
				return false;
			} else if (obj.has("histograph")) {
				return true;
			} else {
				throw new IOException("Unexpected Jest response received while trying to delete index: " + obj.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return false;
		}
	}
	
	public static JSONObject addPIT(JestClient client, Map<String, String> params, Configuration config) throws Exception {
		if (params.containsKey(HistographTokens.PITTokens.DATA)) {
			params.remove(HistographTokens.PITTokens.DATA);
		}
		
		// Terrible workaround to cope with conflicting escape characters with ES. TODO improvement!
		String jsonString = createJSONobject(params).toString();
		JestResult result = client.execute(new Index.Builder(jsonString).index(config.ELASTICSEARCH_INDEX).type(config.ELASTICSEARCH_TYPE).id(params.get(HistographTokens.General.HGID)).build());
		return new JSONObject(result.getJsonString());
	}
	
	public static JSONObject updatePIT(JestClient client, Map<String, String> params, Configuration config) throws Exception {
		deletePIT(client, params, config);
		return addPIT(client, params, config);
	}
	
	public static JSONObject deletePIT(JestClient client, Map<String, String> params, Configuration config) throws Exception {
		JestResult result = client.execute(new Delete.Builder(params.get(HistographTokens.General.HGID)).index(config.ELASTICSEARCH_INDEX).type(config.ELASTICSEARCH_TYPE).build());
		return new JSONObject(result.getJsonString());
	}
	
	private static JSONObject createJSONobject(Map<String, String> params) {
		JSONObject out = new JSONObject();
		
		out.put(HistographTokens.General.HGID, params.get(HistographTokens.General.HGID));
		out.put(HistographTokens.General.SOURCE, params.get(HistographTokens.General.SOURCE));
		out.put(HistographTokens.PITTokens.NAME, params.get(HistographTokens.PITTokens.NAME));
		out.put(HistographTokens.PITTokens.TYPE, params.get(HistographTokens.PITTokens.TYPE));
		
		if (params.containsKey(HistographTokens.PITTokens.GEOMETRY)) {
			JSONObject geom = new JSONObject(params.get(HistographTokens.PITTokens.GEOMETRY));
			out.put(HistographTokens.PITTokens.GEOMETRY, geom);
		}
		if (params.containsKey(HistographTokens.PITTokens.URI)) {
			out.put(HistographTokens.PITTokens.URI, params.get(HistographTokens.PITTokens.URI));			
		}
		if (params.containsKey(HistographTokens.PITTokens.STARTDATE)) {
			out.put(HistographTokens.PITTokens.STARTDATE, params.get(HistographTokens.PITTokens.STARTDATE));			
		}
		if (params.containsKey(HistographTokens.PITTokens.ENDDATE)) {
			out.put(HistographTokens.PITTokens.ENDDATE, params.get(HistographTokens.PITTokens.ENDDATE));			
		}
		
		return out;
	}
}
