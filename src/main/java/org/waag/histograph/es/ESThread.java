package org.waag.histograph.es;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.waag.histograph.util.Configuration;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;

public class ESThread implements Runnable {

	static Configuration config;
	
	public ESThread (Configuration config) {
		ESThread.config = config;
	}
	
	public static void start (String argv[]) {		
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT).multiThreaded(true).build());
		JestClient client = factory.getObject();

		// Create index
		try {
			client.execute(new CreateIndex.Builder("histograph").build());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String mappingFilePath = config.SCHEMA_DIR + "/elasticsearch/pit.json";
		try {
			String mapping = new String(Files.readAllBytes(Paths.get(mappingFilePath)));
		} catch (IOException e) {
			e.printStackTrace();
		}


	}
		
	public void run() {
		
	}

}
