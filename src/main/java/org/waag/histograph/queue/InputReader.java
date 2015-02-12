package org.waag.histograph.queue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.json.JSONException;
import org.json.JSONObject;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.driver.ResultSet;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

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
			case NDJSONTokens.Types.VERTEX:
				addVertex(data, layer, client);
				break;
			case NDJSONTokens.Types.EDGE:
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
			case NDJSONTokens.Types.VERTEX:
				deleteVertex(data, layer, client);
				break;
			case NDJSONTokens.Types.EDGE:
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

		ResultSet r = submitQuery(client, "g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam)", params);
		verifyVertexAbsent(r, params.get("hgidParam").toString(), layer);
		
		ResultSet r2 = submitQuery(client, "g.addVertex('name', nameParam, 'hgid', hgidParam, 'type', typeParam)", params);
		for (Iterator<Result> i = r2.iterator(); i.hasNext(); ) {
			System.out.println("Got result: " + i.next().getVertex());
		}
	}
	
	public static void addEdge(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getEdgeParams(data, layer);
		
		// Query 1: Get and verify Vertex OUT
		ResultSet r = submitQuery(client, "g.V().has('hgid', fromParam)", params);
		verifyVertexExists(r, params.get("fromParam").toString());
		
		// Query 2: Get and verify Vertex IN
		ResultSet r2 = submitQuery(client, "g.V().has('hgid', toParam)", params);
		verifyVertexExists(r2, params.get("toParam").toString());		
		
		// Query 3: Verify the absence of the new edge
		ResultSet r3 = submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, typeParam).has('layer', layerParam).inV().has('hgid', toParam)", params);
		verifyEdgeAbsent(r3, params);
		
		// Query 4: Create edge between Vertex OUT to IN with layer
		ResultSet r4 = submitQuery(client, "g.V().has('hgid', fromParam).next().addEdge(typeParam, g.V().has('hgid', toParam).next(), 'layer', layerParam)", params);

//		// Test
//		ResultSet rt = client.submitAsync("g.V().has('hgid', fromParam).map {g.V().has('hgid', toParam).tryGet().orElse(false)}.is(neq, false).addInE(typeParam, fromParam)", params).get();
		
		for (Iterator<Result> i = r4.iterator(); i.hasNext(); ) {
			System.out.println("Got result: " + i.next());
		}
	}
	
	private static Vertex verifyVertexExists(ResultSet r, String hgID) throws IOException {
		Iterator<Result> i = r.iterator();
		if (!i.hasNext()) throw new IOException ("Vertex with hgID '" + hgID + "' not found in graph.");
		Vertex v = i.next().getVertex();
		if (i.hasNext()) throw new IOException ("Multiple vertices with hgID '" + hgID + "' found in graph.");
		return v;
	}
	
	private static Edge verifyEdgeExists(ResultSet r, Map<String, Object> params) throws IOException {
		Iterator<Result> i = r.iterator();
		String edgeName = params.get("fromParam").toString() + " --" + params.get("typeParam").toString() + "--> " + params.get("toParam");
		if (!i.hasNext()) throw new IOException ("Edge '" + edgeName + "' not found in graph.");
		Edge e = i.next().getEdge();
		if (i.hasNext()) throw new IOException ("Multiple edges '" + edgeName + "' found in graph.");
		return e;
	}
	
	private static void verifyVertexAbsent(ResultSet r, String hgID, String layer) throws IOException {
		Iterator<Result> i = r.iterator();
		if (i.hasNext()) throw new IOException ("Vertex with hgID '" + hgID + "' from layer '" + layer + "' already exists.");
	}
	
	private static void verifyEdgeAbsent(ResultSet r, Map<String, Object> params) throws IOException {
		Iterator<Result> i = r.iterator();
		if (i.hasNext()) {
			String edgeName = params.get("fromParam").toString() + " --" + params.get("typeParam").toString() + "--> " + params.get("toParam");
			throw new IOException ("Edge '" + edgeName + "' from layer '" + params.get("layerParam") + "' already exists.");
		}
	}

	public static void deleteVertex(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getVertexParams(data, layer);
		
		// Get and verify vertex
		ResultSet r = submitQuery(client, "g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam)", params);
		verifyVertexExists(r, params.get("hgidParam").toString());
		
		// Remove vertex
		submitQuery(client, "g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam).remove()", params);
		System.out.println("Vertex successfully deleted.");
	}
	
	public static void deleteEdge(JSONObject data, String layer, Client client) throws IOException {
		Map<String, Object> params = getEdgeParams(data, layer);

		// Verify edge exists
		ResultSet r = submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, typeParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x')", params);
		verifyEdgeExists(r, params);
		
		// Remove edge
		submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, typeParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x').remove()", params);
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
			map.put("typeParam", data.get(NDJSONTokens.EdgeTokens.LABEL).toString());
			map.put("layerParam", layer);
			return map;
		} catch (JSONException e) {
			throw new IOException("Edge token(s) missing (from / to / type).");
		}
	}
	
	private static ResultSet submitQuery(Client client, String query, Map<String, Object> params) throws IOException {
		try {
			return client.submitAsync(query, params).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("Exception while executing remote query:", e);
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