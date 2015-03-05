package org.waag.histograph.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.Status;
import io.searchbox.indices.aliases.GetAliases;
import io.searchbox.indices.mapping.PutMapping;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONObject;
import org.waag.histograph.util.Configuration;

public class ESInit {
	
	public static JestClient initES (Configuration config) {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT).multiThreaded(true).build());
		JestClient client = factory.getObject();
		
		testConnection(client, config);
				
		if (!indexExists(client, config)) {
			createIndex(client, config);
		}
		
		return client;
	}
	
	private static void testConnection (JestClient client, Configuration config) {
		try {
			client.execute(new Status.Builder().build());
		} catch (Exception e) {
			System.out.println("Error: Could not connect to Elasticsearch at http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT);
			System.exit(1);
		}
	}
		
	private static boolean indexExists (JestClient client, Configuration config) {
		JestResult result;
		try {
			result = client.execute(new GetAliases.Builder().build());
			JSONObject obj = new JSONObject(result.getJsonString());
			return (obj.length() > 0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return false;
		}
	}

	private static void createIndex (JestClient client, Configuration config) {		
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
}