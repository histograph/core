package org.waag.histograph.es;

import java.io.IOException;

import org.waag.histograph.util.Configuration;

import io.searchbox.client.JestClient;
import io.searchbox.indices.DeleteIndex;

public class RemoveIndex {

	static Configuration config;
	
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
		JestClient client = ESInit.initES(config);
		
		ESMethods.testConnection(client, config);
		
		try {
			if (ESMethods.indexExists(client, config)) {
				client.execute(new DeleteIndex.Builder(config.ELASTICSEARCH_INDEX).build());
				if (!ESMethods.indexExists(client, config)) {
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
}