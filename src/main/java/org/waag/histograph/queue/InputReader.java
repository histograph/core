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

		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam)", params);
		ClientMethods.verifyVertexAbsent(r, params.get("hgidParam").toString(), layer);
		
		ResultSet r2 = ClientMethods.submitQuery(client, "g.addVertex('name', nameParam, 'hgid', hgidParam, 'type', typeParam)", params);
		for (Iterator<Result> i = r2.iterator(); i.hasNext(); ) {
			System.out.println("Got result: " + i.next().getVertex());
		}
	}
	
	public static void addEdge(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getEdgeParams(data, layer);
		
		// Query 1: Get and verify Vertex OUT
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam)", params);
		ClientMethods.verifyVertexExists(r, params.get("fromParam").toString());
		
		// Query 2: Get and verify Vertex IN
		ResultSet r2 = ClientMethods.submitQuery(client, "g.V().has('hgid', toParam)", params);
		ClientMethods.verifyVertexExists(r2, params.get("toParam").toString());		
		
		// Query 3: Verify the absence of the new edge
		ResultSet r3 = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).inV().has('hgid', toParam)", params);
		ClientMethods.verifyEdgeAbsent(r3, params);
		
		// Query 4: Create edge between Vertex OUT to IN with layer
		ResultSet r4 = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).next().addEdge(labelParam, g.V().has('hgid', toParam).next(), 'layer', layerParam)", params);

//		// Test
//		ResultSet rt = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).map {g.V().has('hgid', toParam).tryGet().orElse(false)}.is(neq, false).addInE(labelParam, fromParam)", params);
		
		for (Iterator<Result> i = r4.iterator(); i.hasNext(); ) {
			System.out.println("Got result: " + i.next());
		}
		
		// Infer atomic relations from edge
		AtomicInferencer.inferAtomic(client, params);
	}

	public static void deleteVertex(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getVertexParams(data, layer);
		
		// Get and verify vertex
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam)", params);
		ClientMethods.verifyVertexExists(r, params.get("hgidParam").toString());
		
		// Remove vertex
		ClientMethods.submitQuery(client, "g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam).remove()", params);
		System.out.println("Vertex successfully deleted.");
	}
	
	public static void deleteEdge(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getEdgeParams(data, layer);

		// Verify edge exists
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x')", params);
		ClientMethods.verifyEdgeExists(r, params);
		
		// Remove edge
		ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x').remove()", params);
		System.out.println("Edge successfully deleted.");
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