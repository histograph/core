package org.waag.histograph.queue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.json.JSONException;
import org.json.JSONObject;

import com.tinkerpop.gremlin.driver.Client;

public class InputReader {

	public static void parse(JSONObject obj, Client client) throws IOException, JSONException {
		switch ((String) obj.get("action")) {
		case "add":
			parseAdd(obj, client);
			break;
		case "delete":
			parseDelete(obj, client);
			break;
		case "update":
			parseUpdate(obj, client);
			break;
		default:
			throw new IOException("Invalid action received: " + (String) obj.get("action"));
		}
	}
	
	public static void parseAdd(JSONObject obj, Client client) throws IOException, JSONException {
		switch ((String) obj.get("type")) {
		case "vertex":
			addVertex(obj, client);
			break;
		case "edge":
			addEdge(obj, client);
			break;
		default:
			throw new IOException("Invalid type received: " + (String) obj.get("type"));
		}
	}
	
	public static void parseDelete(JSONObject obj, Client client) throws IOException, JSONException {		
		switch ((String) obj.get("type")) {
		case NDJSONTokens.Types.VERTEX:
			deleteVertex(obj, client);
			break;
		case NDJSONTokens.Types.EDGE:
			deleteEdge(obj, client);
			break;
		default:
			throw new IOException("Invalid type received: " + (String) obj.get("type"));
		}
	}
	
	public static void parseUpdate(JSONObject obj, Client client) throws IOException, JSONException {
		System.out.println("Update command not yet implemented.");
	}
	
	public static void addVertex(JSONObject obj, Client client) throws IOException, JSONException {
		JSONObject data = obj.getJSONObject("data");

		String name = (String) data.get(NDJSONTokens.VertexTokens.NAME);
		String id = (String) data.get(NDJSONTokens.VertexTokens.ID);
		String type = (String) data.get(NDJSONTokens.VertexTokens.TYPE);
		
		try {
			client.submitAsync("g.addVertex('name', '" + name + "', 'id', '" + id + "', 'type', '" + type + "')").get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public static void addEdge(JSONObject obj, Client client) throws IOException, JSONException {
//		JSONObject data = obj.getJSONObject("data");
		
//		String from = (String) data.get(NDJSONTokens.EdgeTokens.FROM);
//		String to = (String) data.get(NDJSONTokens.EdgeTokens.TO);
//		String type = (String) data.get(NDJSONTokens.EdgeTokens.TYPE);
	}
	
	public static void deleteVertex(JSONObject obj, Client client) throws IOException, JSONException {
		System.out.println("Delete vertex command not yet implemented.");
	}
	
	public static void deleteEdge(JSONObject obj, Client client) throws IOException, JSONException {
		System.out.println("Delete edge command not yet implemented.");
	}
}