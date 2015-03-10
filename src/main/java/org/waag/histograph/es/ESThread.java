package org.waag.histograph.es;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.json.JSONObject;
import org.waag.histograph.queue.QueueTask;
import org.waag.histograph.util.HistographTokens;

import io.searchbox.client.JestClient;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;

public class ESThread implements Runnable {

	JestClient client;
	BlockingQueue<QueueTask> queue;
	String index;
	String type;
	boolean verbose;
	
	public ESThread (JestClient client, BlockingQueue<QueueTask> queue, String index, String type, boolean verbose) {
		this.client = client;
		this.queue = queue;
		this.index = index;
		this.type = type;
		this.verbose = verbose;
	}
	
	public void run () {
		try {
			int tasksDone = 0;
			while (true) {
				QueueTask task = queue.take();
				
				try {
					performTask(task);
				} catch (Exception e) {
					writeToFile("esErrors.txt", "Error: ", e.getMessage());
				}
				
				if (verbose) {
					tasksDone ++;
					if (tasksDone % 100 == 0) {
						int tasksLeft = queue.size();
						System.out.println("[ESThread] Processed " + tasksDone + " tasks -- " + tasksLeft + " left in queue.");
					}
				}
			}
		} catch (InterruptedException e) {
			System.out.println("ES thread interrupted!");
			System.exit(1);
		}
	}
	
	private void performTask(QueueTask task) throws Exception {
		switch (task.getType()) {
		case HistographTokens.Types.PIT:
			performPITAction(task);
			break;
		case HistographTokens.Types.RELATION:
			// Relations are not put into elasticsearch.
			break;
		default:
			throw new IOException("Unexpected type received.");
		}
	}
	
	private void performPITAction(QueueTask task) throws Exception {
		Map<String, String> params = task.getParams();
		switch (task.getAction()) {
		case HistographTokens.Actions.ADD:
			addPIT(params);
			break;
		case HistographTokens.Actions.UPDATE:
			updatePIT(params);
			break;
		case HistographTokens.Actions.DELETE:
			deletePIT(params);
			break;
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
	
	private void addPIT(Map<String, String> params) throws Exception {
		if (params.containsKey(HistographTokens.PITTokens.DATA)) {
			params.remove(HistographTokens.PITTokens.DATA);
		}
		
		// Terrible workaround to cope with conflicting escape characters with ES
		String jsonString = createJSONobject(params).toString();
		client.execute(new Index.Builder(jsonString).index(index).type(type).id(params.get(HistographTokens.General.HGID)).build());	
	}
	
	private void updatePIT(Map<String, String> params) throws Exception {
		deletePIT(params);
		addPIT(params);
	}
	
	private void deletePIT(Map<String, String> params) throws Exception {
		client.execute(new Delete.Builder(params.get(HistographTokens.General.HGID)).index(index).type(type).build());
	}
	
	private JSONObject createJSONobject(Map<String, String> params) {
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