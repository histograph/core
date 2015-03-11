package org.waag.histograph.es;

import java.io.FileWriter;
import java.io.IOException;
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

public class ESThread implements Runnable {

	JestClient client;
	Configuration config;
	boolean verbose;
	
	public ESThread (JestClient client, Configuration config, boolean verbose) {
		this.client = client;
		this.config = config;
		this.verbose = verbose;
	}
	
	public void run () {
		Jedis jedis = RedisInit.initRedis();
		List<String> messages = null;
		String payload = null;
		int tasksDone = 0;
		
		System.out.println("[ESThread] Ready to take messages.");
		while (true) {
			Task task = null;
			
			try {
				messages = jedis.blpop(0, config.REDIS_ES_QUEUE);
				payload = messages.get(1);
			} catch (JedisConnectionException e) {
				System.out.println("[ESThread] Redis connection error: " + e.getMessage());
				System.exit(1);
			}
			
			try {
				JSONObject jsonMessage = new JSONObject(payload);
				task = InputReader.parse(jsonMessage);
			} catch (IOException e) {
				writeToFile("esMsgParseErrors.txt", "Error: ", e.getMessage());
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
					System.out.println("[ESThread] Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
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
			return ESMethods.addPIT(client, params, config);
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
			fileOut.write(header + message + "\n");
			fileOut.close();
		} catch (Exception e) {
			System.out.println("Unable to write '" + message + "' to file '" + fileName + "'.");
		}	
	}
}