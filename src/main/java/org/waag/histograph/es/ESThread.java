package org.waag.histograph.es;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.waag.histograph.queue.RedisInit;
import org.waag.histograph.queue.Task;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.InputReader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import io.searchbox.client.JestClient;

/**
 * A single thread class that keeps polling a Redis queue for Elasticsearch operations and updates the Elasticsearch index accordingly.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class ESThread implements Runnable {

	JestClient client;
	Configuration config;
	boolean verbose;
	private static final String NAME = "ESThread";
	
	/**
	 * Constructor of the thread.
	 * @param client The Elasticsearch client object on which the operations should be performed.
	 * @param config The {@link Configuration} object in which the Elasticsearch parameters are defined.
	 * @param verbose Boolean value expressing whether the Elasticsearch thread should write verbose output to stdout.
	 */
	public ESThread (JestClient client, Configuration config, boolean verbose) {
		this.client = client;
		this.config = config;
		this.verbose = verbose;
	}
	
	/**
	 * The thread's main loop. Keeps polling the Redis queue for tasks and performs them synchronously.
	 * Errors are written to <i>esErrors.txt</i> and Redis queue parsing errors to <i>esMsgParseErrors.txt</i>.
	 */
	public void run () {
		Jedis jedis = null;
		try {
			jedis = RedisInit.initRedis();
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			System.exit(1);
		}
		
		List<String> messages = null;
		String payload = null;
		int tasksDone = 0;
		
		namePrint("Ready to take messages.");
		while (true) {
			Task task = null;
			
			try {
				messages = jedis.blpop(0, config.REDIS_ES_QUEUE);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				namePrint("Redis connection error: " + e.getMessage());
				System.exit(1);
			}
			
			try {
				JSONObject jsonMessage = new JSONObject(payload);
				task = InputReader.parse(jsonMessage);
			} catch (IOException e) {
				namePrint("Error: " + e.getMessage());
				writeToFile("esMsgParseErrors.txt", "Error: ", e.getMessage());
				continue;
			}
			
			try {
				performTask(task);
			} catch (Exception e) {
				writeToFile("esErrors.txt", "Error: ", e.getMessage());
			}
			
			if (verbose) {
				tasksDone ++;
				if (tasksDone % 100 == 0) {
					int tasksLeft = jedis.llen(config.REDIS_ES_QUEUE).intValue();
					namePrint("Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
				}
			}
		}
	}
	
	private void performTask(Task task) throws Exception {
		switch (task.getType()) {
		case HistographTokens.Types.PIT:
			JSONObject response = performPITAction(task);		
			if (response.has("error")) {
				throw new Exception("Could not add PIT with hgid '" + task.getParams().get(HistographTokens.General.HGID) + "'. Error message: " + response.getString("error"));
			}
			break;
		case HistographTokens.Types.RELATION:
			// Relations are not added to elasticsearch.
			break;
		default:
			throw new IOException("Unexpected type received.");
		}
	}
	
	private JSONObject performPITAction(Task task) throws Exception {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			JSONObject response = ESMethods.addPIT(client, params, config);
			// Workaround to make sure PIT is added after it failed because of its geometry value
			if (response.has("error") && response.getString("error").contains("failed to parse [" + HistographTokens.PITTokens.GEOMETRY + "]")) {
				params.remove(HistographTokens.PITTokens.GEOMETRY);
				JSONObject response2 = ESMethods.addPIT(client, params, config);
				if (response2.has("error")) {
					throw new Exception("Could not add PIT with hgid '" + params.get(HistographTokens.General.HGID) + "', after removing geometry. Error message: " + response2.getString("error"));
				} else {
					throw new Exception("Geometry of PIT with hgid '" + params.get(HistographTokens.General.HGID) + "' was rejected. Error message: " + response.getString("error"));
				}
			} else {
				return response;
			}
		case HistographTokens.Actions.UPDATE:
			return ESMethods.updatePIT(client, params, config);
		case HistographTokens.Actions.DELETE:
			return ESMethods.deletePIT(client, params, config);
		default:
			throw new IOException("Unexpected action received.");
		}
	}
	
	private void writeToFile(String fileName, String header, String message) {
		try {
			FileWriter fileOut = new FileWriter(fileName, true);
			Date now = new Date();
			SimpleDateFormat format = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss] ");
			fileOut.write(format.format(now) + header + message + "\n");
			fileOut.close();
		} catch (Exception e) {
			namePrint("Unable to write '" + message + "' to file '" + fileName + "'.");
		}	
	}
	
	private void namePrint(String message) {
		System.out.println("[" + NAME + "] " + message);
	}
}