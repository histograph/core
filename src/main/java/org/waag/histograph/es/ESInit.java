package org.waag.histograph.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.waag.histograph.util.Configuration;

public class ESInit {
	
	public static JestClient initES (Configuration config) {
		JestClientFactory factory = new JestClientFactory();
		String url = "http://" + config.ELASTICSEARCH_HOST + ":" + config.ELASTICSEARCH_PORT;
		factory.setHttpClientConfig(new HttpClientConfig.Builder(url)
														.multiThreaded(true)
														.readTimeout(10000)
														.build());
		JestClient client = factory.getObject();
		
		ESMethods.testConnection(client, config);
		
		return client;
	}
}