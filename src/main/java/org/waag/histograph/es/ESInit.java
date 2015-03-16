package org.waag.histograph.es;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import org.waag.histograph.util.Configuration;

/**
 * A wrapper class that initializes the Elasticsearch client, based on the provided configuration parameters.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class ESInit {
	
	/**
	 * Static method that creates an Elasticsearch client 
	 * @param config Configuration object containing Elasticsearch connection parameters.
	 * @return Returns a {@link JestClient} object with a succesfully established Elasticsearch connection.
	 * @throws Exception Thrown if the connection of the newly created client fails.
	 */
	public static JestClient initES (Configuration config) throws Exception {
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