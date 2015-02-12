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
import com.tinkerpop.gremlin.structure.Vertex;

public class InputReader {
	
	public static void parse(JSONObject obj, Client client) throws IOException {
		String source;
		
		try {
			source = obj.get(NDJSONTokens.General.SOURCE).toString();
			switch (obj.get(NDJSONTokens.General.ACTION).toString()) {
			case NDJSONTokens.Actions.ADD:
				parseAdd(obj, source, client);
				break;
			case NDJSONTokens.Actions.DELETE:
				parseDelete(obj, source, client);
				break;
			case NDJSONTokens.Actions.UPDATE:
				parseUpdate(obj, source, client);
				break;
			default:
				throw new IOException("Invalid action received: " + obj.get(NDJSONTokens.General.ACTION).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No source or action in JSON input.");
		}	
	}
	
	public static void parseAdd(JSONObject obj, String source, Client client) throws IOException {
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.VERTEX:
				addVertex(data, source, client);
				break;
			case NDJSONTokens.Types.EDGE:
				addEdge(data, source, client);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	public static void parseDelete(JSONObject obj, String source, Client client) throws IOException {		
		JSONObject data;
		try {
			data = obj.getJSONObject(NDJSONTokens.General.DATA);
		} catch (JSONException e) {
			throw new IOException("No data in JSON input.");
		}
		
		try {
			switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
			case NDJSONTokens.Types.VERTEX:
				deleteVertex(data, source, client);
				break;
			case NDJSONTokens.Types.EDGE:
				deleteEdge(data, source, client);
				break;
			default:
				throw new IOException("Invalid type received: " + obj.get(NDJSONTokens.General.TYPE).toString());
			}
		} catch (JSONException e) {
			throw new IOException("No type in JSON input.");
		}
	}
	
	public static void parseUpdate(JSONObject obj, String source, Client client) throws IOException {
		throw new IOException("Update command not yet implemented.");
	}
	
	public static void addVertex(JSONObject data, String source, Client client) throws IOException {		
		try {
			Map<String, Object> params = new HashMap<String, Object>();
			
			params.put("nameParam", data.get(NDJSONTokens.VertexTokens.NAME).toString());
			params.put("hgidParam", source + "/" + data.get(NDJSONTokens.VertexTokens.ID).toString());
			params.put("typeParam", data.get(NDJSONTokens.VertexTokens.TYPE).toString());

			ResultSet r = client.submitAsync("g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam)", params).get();
			verifyVertexAbsent(r, params.get("hgidParam").toString(), source);
			
			ResultSet r2 = client.submitAsync("g.addVertex('name', nameParam, 'hgid', hgidParam, 'type', typeParam)", params).get();
			for (Iterator<Result> i = r2.iterator(); i.hasNext(); ) {
				System.out.println("Got result: " + i.next().getVertex());
			}
		} catch (JSONException e) {
			throw new IOException("Vertex token(s) missing (name / id / type).");
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("Exception while executing remote query:", e);
		}
	}
	
	public static void addEdge(JSONObject data, String source, Client client) throws IOException {
		try {			
			Map<String, Object> params = new HashMap<String, Object>();
			
			String fromHGid = parseHGid(source, data.get(NDJSONTokens.EdgeTokens.FROM).toString());
			String toHGid = parseHGid(source, data.get(NDJSONTokens.EdgeTokens.TO).toString());
			String type = data.get(NDJSONTokens.EdgeTokens.TYPE).toString();
	
			params.put("fromParam", fromHGid);
			params.put("toParam", toHGid);
			params.put("typeParam", type);
			params.put("sourceParam", source);
			
			// Query 1: Get and verify Vertex OUT
			ResultSet r = client.submitAsync("g.V().has('hgid', fromParam)", params).get();
			verifyVertexExists(r, fromHGid);
			
			// Query 2: Get and verify Vertex IN
			ResultSet r2 = client.submitAsync("g.V().has('hgid', toParam)", params).get();
			verifyVertexExists(r2, fromHGid);		
			
			// Query 3: Verify the absence of the new edge
			ResultSet r3 = client.submitAsync("g.V().has('hgid', fromParam).outE().has(label, typeParam).has('source', sourceParam).inV().has('hgid', toParam)", params).get();
			verifyEdgeAbsent(r3, fromHGid, type, toHGid, source);
			
			// Query 4: Create edge between Vertex OUT to IN with source
			ResultSet r4 = client.submitAsync("g.V().has('hgid', fromParam).next().addEdge(typeParam, g.V().has('hgid', toParam).next(), 'source', sourceParam)", params).get();

//			// Test
//			ResultSet rt = client.submitAsync("g.V().has('hgid', fromParam).map {g.V().has('hgid', toParam).tryGet().orElse(false)}.is(neq, false).addInE(typeParam, fromParam)", params).get();
			
			for (Iterator<Result> i = r4.iterator(); i.hasNext(); ) {
				System.out.println("Got result: " + i.next());
			}
		} catch (JSONException e) {
			throw new IOException("Edge token(s) missing (from / to / type).");
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("Exception while executing remote query:", e);
		}
	}
	
	private static Vertex verifyVertexExists(ResultSet r, String hgID) throws IOException {
		Iterator<Result> i = r.iterator();
		if (!i.hasNext()) throw new IOException ("Vertex with hgID '" + hgID + "' not found in graph.");
		Vertex v = i.next().getVertex();
		if (i.hasNext()) throw new IOException ("Multiple vertices with hgID '" + hgID + "' found in graph.");
		return v;
	}
	
	private static void verifyVertexAbsent(ResultSet r, String hgID, String source) throws IOException {
		Iterator<Result> i = r.iterator();
		if (i.hasNext()) throw new IOException ("Vertex with hgID '" + hgID + "' from source '" + source + "' already exists.");
	}
	
	private static void verifyEdgeAbsent(ResultSet r, String from, String type, String to, String source) throws IOException {
		Iterator<Result> i = r.iterator();
		if (i.hasNext()) throw new IOException ("Edge '" + from + " --" + type + "--> " + to + "' from source '" + source + "' already exists.");
	}

	public static void deleteVertex(JSONObject data, String source, Client client) throws IOException {
		Map<String, Object> params = new HashMap<String, Object>();
		
		try {
			params.put("nameParam", data.get(NDJSONTokens.VertexTokens.NAME).toString());
			params.put("hgidParam", source + "/" + data.get(NDJSONTokens.VertexTokens.ID).toString());
			params.put("typeParam", data.get(NDJSONTokens.VertexTokens.TYPE).toString());
		} catch (JSONException e) {
			throw new IOException("Vertex token(s) missing (name / id / type).");
		}
		
		try {
			// Get and verify vertex
			ResultSet r = client.submitAsync("g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam)", params).get();
			verifyVertexExists(r, params.get("hgidParam").toString());
			
			// Remove vertex
			client.submitAsync("g.V().has('hgid', hgidParam).has('name', nameParam).has('type', typeParam).remove()", params).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("Exception while executing remote query:", e);
		}
		System.out.println("Vertex successfully deleted.");
	}
	
	public static void deleteEdge(JSONObject data, String source, Client client) throws IOException {
		throw new IOException("Delete edge command not yet implemented.");
	}
	
	private static String parseHGid (String source, String id) {
		if (isNumeric(id)) {
			return source + "/" + id;
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