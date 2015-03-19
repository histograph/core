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

/**
 * A class containing several static methods for basic Elasticsearch operations. All methods
 * require an added {@link JestClient} object that handles the ES connection.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class ESMethods {
	
	/**
	 * Method that tests the ES client's connection. An exception is thrown if the connection fails.
	 * @param client The client object that handles the ES connection.
	 * @param config {@link Configuration} object containing the configuration of the JestClient object.
	 * @throws Exception Thrown when the connection fails.
	 */
	public static void testConnection (JestClient client, Configuration config) throws Exception {
		try {
			client.execute(new Status.Builder().build());
		} catch (Exception e) {
			throw new Exception("Could not connect to Elasticsearch at http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT);
		}
	}
	
	/**
	 * Method that creates a new PIT index. The mapping file location should be defined in the Configuration object.
	 * @param client The client object that handles the ES connection.
	 * @param config {@link Configuration} object containing the configuration of the JestClient object.
	 * @throws Exception Thrown if index creation fails.
	 */
	public static void createIndex (JestClient client, Configuration config) throws Exception {		
		try {
			client.execute(new CreateIndex.Builder(config.ELASTICSEARCH_INDEX).build());
		} catch (Exception e) {
			throw new Exception("Unable to create index. " + e.getMessage());
		}
		
		String mappingFilePath = config.SCHEMA_DIR + "/elasticsearch/pit.json";

		try {
			String mapping = new String(Files.readAllBytes(Paths.get(mappingFilePath)));
			PutMapping putMapping = new PutMapping.Builder(config.ELASTICSEARCH_INDEX, config.ELASTICSEARCH_TYPE, mapping).build();
			client.execute(putMapping);
		} catch (IOException e) {
			throw new Exception("Unable to read mapping file. " + e.getMessage());
		} catch (Exception e) {
			throw new Exception("Unable to put mapping. " + e.getMessage());
		}
	}
	
	/**
	 * Method that checks whether the Histograph index exists. Index and type should be specified in the Configuration object.
	 * @param client The JestClient object containing the Elasticsearch connection
	 * @param config The Configuration object in which the Elasticsearch index and type are defined
	 * @return A boolean value expressing whether the index exists or not.
	 * @throws Exception Thrown when an unexpected reponse is given by the JestClient
	 */
	public static boolean indexExists (JestClient client, Configuration config) throws Exception {
		JestResult result;
		try {
			result = client.execute(new GetMapping.Builder().addIndex(config.ELASTICSEARCH_INDEX).build());
			JSONObject obj = new JSONObject(result.getJsonString());

			if (obj.has("error") && obj.has("status") && obj.getInt("status") == 404) {
				return false;
			} else if (obj.has("histograph")) {
				return true;
			} else {
				throw new IOException("Unexpected Jest response received while trying to poll index: " + obj.toString());
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	
	/**
	 * Method to add a PIT to the Elasticsearch index.
	 * @param client The JestClient object containing the Elasticsearch connection
	 * @param params A map containing the parameters of the PIT that is to be added
	 * @param config A Configuration object in which the Elasticsearch index and type are defined
	 * @return A {@link JSONObject} containing the JestClient's response
	 * @throws Exception Propagated exception from the {@link JestClient#execute(io.searchbox.action.Action)} method.
	 */
	public static JSONObject addPIT(JestClient client, Map<String, String> params, Configuration config) throws Exception {
		if (params.containsKey(HistographTokens.PITTokens.DATA)) {
			params.remove(HistographTokens.PITTokens.DATA);
		}
		
		// Terrible workaround to cope with conflicting escape characters with ES. TODO improvement!
		String jsonString = createJSONobject(params).toString();
		JestResult result = client.execute(new Index.Builder(jsonString).index(config.ELASTICSEARCH_INDEX).type(config.ELASTICSEARCH_TYPE).id(params.get(HistographTokens.General.HGID)).build());
		return new JSONObject(result.getJsonString());
	}
	
	/**
	 * Method to update a PIT in the Elasticsearch index. This is composed of a removal of the old PIT and an addition of the PIT
	 * present in the parameters.
	 * @param client The JestClient object containing the Elasticsearch connection
	 * @param params A map containing the parameters of the PIT that is to be added
	 * @param config A Configuration object in which the Elasticsearch index and type are defined
	 * @return A {@link JSONObject} containing the JestClient's response for both the delete and add operation.
	 * @throws Exception Propagated exception from the {@link JestClient#execute(io.searchbox.action.Action)} methods.
	 */
	public static JSONObject updatePIT(JestClient client, Map<String, String> params, Configuration config) throws Exception {
		JSONObject delResponse = deletePIT(client, params, config);
		JSONObject addResponse = addPIT(client, params, config);
		JSONObject response = new JSONObject();
		response.put("addResponse", addResponse);
		response.put("delResponse", delResponse);
		return response;
	}
	
	/**
	 * Method to delete a PIT from the Elasticsearch index.
	 * @param client The JestClient object containing the Elasticsearch connection
	 * @param params A map containing the parameters of the PIT that is to be removed
	 * @param config A Configuration object in which the Elasticsearch index and type are defined
	 * @return A {@link JSONObject} containing the JestClient's response
	 * @throws Exception Propagated exception from the {@link JestClient#execute(io.searchbox.action.Action)} method.
	 */
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
