package org.waag.histograph.es;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.json.JSONObject;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.queue.QueueAction;

import io.searchbox.client.JestClient;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;

public class ESThread implements Runnable {

	JestClient client;
	BlockingQueue<QueueAction> queue;
	String index;
	String type;
	boolean verbose;
	
	public ESThread (JestClient client, BlockingQueue<QueueAction> queue, String index, String type, boolean verbose) {
		this.client = client;
		this.queue = queue;
		this.index = index;
		this.type = type;
		this.verbose = verbose;
	}
	
	public void run () {
		try {
			int actionsDone = 0;
			while (true) {
				QueueAction action = queue.take();
				
				try {
					performAction(action);
				} catch (Exception e) {
					writeToFile("esErrors.txt", "Error: ", e.getMessage());
				}
				
				if (verbose) {
					actionsDone ++;
					if (actionsDone % 100 == 0) {
						int actionsLeft = queue.size();
						System.out.println("[ESThread] Processed " + actionsDone + " actions -- " + actionsLeft + " left in queue.");
					}
				}
			}
		} catch (InterruptedException e) {
			System.out.println("ES thread interrupted!");
			System.exit(1);
		}
	}
	
	private void performAction(QueueAction action) throws Exception {
		switch (action.getType()) {
		case NDJSONTokens.Types.PIT:
			performPITAction(action);
			break;
		case NDJSONTokens.Types.RELATION:
			// Relations are not put into elasticsearch.
			break;
		default:
			throw new IOException("Unexpected type received.");
		}
	}
	
	private void performPITAction(QueueAction action) throws Exception {
		Map<String, String> params = action.getParams();
		switch (action.getAction()) {
		case NDJSONTokens.Actions.ADD:
			addVertex(params);
			break;
		case NDJSONTokens.Actions.UPDATE:
			updateVertex(params);
			break;
		case NDJSONTokens.Actions.DELETE:
			deleteVertex(params);
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
	
	private void addVertex(Map<String, String> params) throws Exception {
		if (params.containsKey(NDJSONTokens.PITTokens.DATA)) {
			params.remove(NDJSONTokens.PITTokens.DATA);
		}
		
		// Terrible workaround to cope with conflicting escape characters with ES
		String jsonString = createJSONobject(params).toString();
		client.execute(new Index.Builder(jsonString).index(index).type(type).id(params.get(NDJSONTokens.General.HGID)).build());	
	}
	
	private void updateVertex(Map<String, String> params) throws Exception {
		deleteVertex(params);
		addVertex(params);
	}
	
	private void deleteVertex(Map<String, String> params) throws Exception {
		client.execute(new Delete.Builder(params.get(NDJSONTokens.General.HGID)).index(index).type(type).build());
	}
	
	private JSONObject createJSONobject(Map<String, String> params) {
		JSONObject out = new JSONObject();
		
		out.put(NDJSONTokens.General.HGID, params.get(NDJSONTokens.General.HGID));
		out.put(NDJSONTokens.General.LAYER, params.get(NDJSONTokens.General.LAYER));
		out.put(NDJSONTokens.PITTokens.NAME, params.get(NDJSONTokens.PITTokens.NAME));
		out.put(NDJSONTokens.PITTokens.TYPE, params.get(NDJSONTokens.PITTokens.TYPE));
		
		if (params.containsKey(NDJSONTokens.PITTokens.GEOMETRY)) {
			JSONObject geom = new JSONObject(params.get(NDJSONTokens.PITTokens.GEOMETRY));
			out.put(NDJSONTokens.PITTokens.GEOMETRY, geom);
		}
		if (params.containsKey(NDJSONTokens.PITTokens.URI)) {
			out.put(NDJSONTokens.PITTokens.URI, params.get(NDJSONTokens.PITTokens.URI));			
		}
		if (params.containsKey(NDJSONTokens.PITTokens.STARTDATE)) {
			out.put(NDJSONTokens.PITTokens.STARTDATE, params.get(NDJSONTokens.PITTokens.STARTDATE));			
		}
		if (params.containsKey(NDJSONTokens.PITTokens.ENDDATE)) {
			out.put(NDJSONTokens.PITTokens.ENDDATE, params.get(NDJSONTokens.PITTokens.ENDDATE));			
		}
		
		return out;
	}
	
}