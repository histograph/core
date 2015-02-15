package org.waag.histograph.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.waag.histograph.reasoner.AtomicInferencer;
import org.waag.histograph.util.ClientMethods;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.driver.ResultSet;

public class InputReader {
	
	public static void parse(JSONObject obj, Client client) throws IOException {
		String layer;
		
		try {
			layer = obj.get(NDJSONTokens.General.LAYER).toString();
			switch (obj.get(NDJSONTokens.General.ACTION).toString()) {
			case NDJSONTokens.Actions.ADD:
				parseAdd(obj, layer, client);
				break;
			case NDJSONTokens.Actions.DELETE:
				parseDelete(obj, layer, client);
				break;
			case NDJSONTokens.Actions.UPDATE:
				parseUpdate(obj, layer, client);
				break;
			default:
				throw new IOException("Invalid action received: " + obj.get(NDJSONTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No layer or action in JSON input.");
		}	
	}
	
	public static void parseAdd(JSONObject obj, String layer, Client client) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				addVertex(data, layer, client);
				break;
			case NDJSONTokens.Types.RELATION:
				addEdge(data, layer, client);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	public static void parseDelete(JSONObject obj, String layer, Client client) throws IOException {		
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.PIT:
				deleteVertex(data, layer, client);
				break;
			case NDJSONTokens.Types.RELATION:
				deleteEdge(data, layer, client);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	public static void parseUpdate(JSONObject obj, String layer, Client client) throws IOException {
		throw new IOException("Update command not yet implemented.");
	}
	
	public static void addVertex(JSONObject data, String layer, Client client) throws IOException {		
		Map<String, Object> params = getVertexParams(data, layer);

		// Verify absence of the new vertex
		if (!ClientMethods.vertexAbsent(client, params.get("hgidParam").toString())) throw new IOException ("Vertex already exists.");
		
		ResultSet r = ClientMethods.submitQuery(client, "g.addVertex('name', nameParam, 'hgid', hgidParam, 'type', typeParam, 'layer', layerParam)", params);
		for (Iterator<Result> i = r.iterator(); i.hasNext(); ) {
			System.out.println("Got result: " + i.next().getVertex());
		}
	}
	
	public static void addEdge(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getEdgeParams(data, layer);
		
		// Verify both vertices exist
		if (!ClientMethods.vertexExists(client, params.get("fromParam").toString())) throw new IOException("Vertex with hgID " + params.get("fromParam").toString() + " not found in graph.");
		if (!ClientMethods.vertexExists(client, params.get("toParam").toString())) throw new IOException("Vertex with hgID " + params.get("toParam").toString() + " not found in graph.");		
		
		// Verify the absence of the new edge
		if (!ClientMethods.edgeAbsent(client, params)) throw new IOException ("Edge already exists.");
		
		// Create edge between vertices with layer
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).next().addEdge(labelParam, g.V().has('hgid', toParam).next(), 'layer', layerParam)", params);
	
		for (Iterator<Result> i = r.iterator(); i.hasNext(); ) {
			System.out.println("Got result: " + i.next());
		}
		
		// Infer atomic relations from edge
		AtomicInferencer.inferAtomic(client, params);
	}

	public static void deleteVertex(JSONObject data, String layer, Client client) throws IOException {
		String hgID;
		try {
			hgID = parseHGid(layer, data.get(NDJSONTokens.VertexTokens.ID).toString());
		} catch (JSONException e) {
			throw new IOException("Vertex id missing.");
		}
				
		// Verify vertex exists
		if (!ClientMethods.vertexExists(client, hgID)) throw new IOException("Vertex with hgID " + hgID + " not found in graph.");
		
		// Remove vertex
		ClientMethods.submitQuery(client, "g.V().has('hgid', '" + hgID + "').remove()", null);
		System.out.println("Vertex successfully deleted.");
	}
	
	public static void deleteEdge(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getEdgeParams(data, layer);

		// Verify edge exists
		if (!ClientMethods.edgeExists(client, params)) throw new IOException("Edge not found in graph.");
		
		// Remove edge
		ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x').remove()", params);
		ClientMethods.waitForEdgeAbsent(client, params); // To prevent concurrency issues
		System.out.println("Edge " + params.get("labelParam").toString() + " successfully deleted.");
		
		// Remove inferred edges
		AtomicInferencer.removeInferredAtomic(client, params);
	}
	
	private static Map<String, Object> getVertexParams(JSONObject data, String layer) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		
		try {
			map.put("nameParam", data.get(NDJSONTokens.VertexTokens.NAME).toString());
			map.put("hgidParam", parseHGid(layer, data.get(NDJSONTokens.VertexTokens.ID).toString()));
			map.put("typeParam", data.get(NDJSONTokens.VertexTokens.TYPE).toString());
			map.put("layerParam", layer);
			return map;
		} catch (JSONException e) {
			throw new IOException("Vertex token(s) missing (name / id / type).");
		}
	}
	
	private static Map<String, Object> getEdgeParams(JSONObject data, String layer) throws IOException {
		Map<String, Object> map = new HashMap<String, Object>();
		
		try {
			map.put("fromParam", parseHGid(layer, data.get(NDJSONTokens.EdgeTokens.FROM).toString()));
			map.put("toParam", parseHGid(layer, data.get(NDJSONTokens.EdgeTokens.TO).toString()));
			map.put("labelParam", data.get(NDJSONTokens.EdgeTokens.LABEL).toString());
			map.put("layerParam", layer);
			return map;
		} catch (JSONException e) {
			throw new IOException("Edge token(s) missing (from / to / type).");
		}
	}
	
	private static String parseHGid (String layer, String id) {
		if (isNumeric(id)) {
			return layer + "/" + id;
		} else {
			return id;
		}
	}
	
	private static boolean isNumeric(String string) { 
		try { 
			Integer.parseInt(string);  
		} catch(NumberFormatException e) {
			return false;  
		}  
		return true;  
	}
}