package org.waag.histograph.es;

import java.io.IOException;

import org.json.JSONObject;
import org.waag.histograph.util.Configuration;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.Status;
import io.searchbox.indices.aliases.GetAliases;

public class RemoveIndex {

	static Configuration config;
	JestClient client;
	
	public static void main(String[] argv) {
		boolean fromFile = false;
		
		for (int i=0; i<argv.length; i++) {
			if (argv[i].equals("-config")) {
				try {
					config = Configuration.fromFile(argv[i+1]);
					fromFile = true;
				} catch (ArrayIndexOutOfBoundsException e) {
					System.out.println("Error: No config file provided.");
					System.exit(1);
				} catch (IOException e) {
					System.out.println("Error: " + e.getMessage());
					System.exit(1);
				}
			}
		}
		
		if (!fromFile) {
			try {
				config = Configuration.fromEnv();
			} catch (IOException e) {
				System.out.println("Error: " + e.getMessage());
				System.exit(1);
			}
		}
		
		new RemoveIndex().start();
	}
	
	private void start () {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT).multiThreaded(true).build());
		client = factory.getObject();
		
		testConnection();
		
		try {
			if (indexExists()) {
				client.execute(new DeleteIndex.Builder("histograph").build());
				if (!indexExists()) {
					System.out.println("Index successfully removed.");
				} else {
					System.out.println("Hmm, something went wrong");
				}
			} else {
				System.out.println("Index did not exist in the first place!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		client.shutdownClient();
	}
	
	private void testConnection () {
		try {
			client.execute(new Status.Builder().build());
		} catch (Exception e) {
			System.out.println("Error: Could not connect to Elasticsearch at http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT);
			System.exit(1);
		}
	}
	
	private boolean indexExists () {
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
}