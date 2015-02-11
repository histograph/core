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
	
	public static void parse(JSONObject obj, Client client) throws IOException, JSONException {
		String source;
		
		try {
			source = obj.get(NDJSONTokens.General.SOURCE).toString();
		} catch (JSONException e) {
			throw new IOException("No source in JSON input.");
		}
		
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
			throw new IOException("Invalid action received: " + (String) obj.get(NDJSONTokens.General.ACTION));
		}
	}
	
	public static void parseAdd(JSONObject obj, String source, Client client) throws IOException, JSONException {
		switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
		case NDJSONTokens.Types.VERTEX:
			addVertex(obj, source, client);
			break;
		case NDJSONTokens.Types.EDGE:
			addEdge(obj, source, client);
			break;
		default:
			throw new IOException("Invalid type received: " + (String) obj.get(NDJSONTokens.General.TYPE));
		}
	}
	
	public static void parseDelete(JSONObject obj, String source, Client client) throws IOException, JSONException {		
		switch (obj.get(NDJSONTokens.General.TYPE).toString()) {
		case NDJSONTokens.Types.VERTEX:
			deleteVertex(obj, source, client);
			break;
		case NDJSONTokens.Types.EDGE:
			deleteEdge(obj, source, client);
			break;
		default:
			throw new IOException("Invalid type received: " + (String) obj.get(NDJSONTokens.General.TYPE));
		}
	}
	
	public static void parseUpdate(JSONObject obj, String source, Client client) throws IOException {
		throw new IOException("Update command not yet implemented.");
	}
	
	public static void addVertex(JSONObject obj, String source, Client client) throws IOException {		
		try {
			JSONObject data = obj.getJSONObject(NDJSONTokens.VertexTokens.DATA);
			Map<String, Object> params = new HashMap<String, Object>();
			
			params.put("nameParam", (String) data.get(NDJSONTokens.VertexTokens.NAME));
			params.put("hgidParam", source + "/" + (String) data.get(NDJSONTokens.VertexTokens.ID));
			params.put("typeParam", (String) data.get(NDJSONTokens.VertexTokens.TYPE));

			ResultSet r = client.submitAsync("g.addVertex('name', nameParam, 'hgid', hgidParam, 'type', typeParam)", params).get();
			for (Iterator<Result> i = r.iterator(); i.hasNext(); ) {
				System.out.println("Got result: " + i.next().getVertex());
			}
		} catch (JSONException e) {
			throw new IOException("Vertex token(s) missing (data / name / id / type).");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	public static void addEdge(JSONObject obj, String source, Client client) throws IOException, JSONException {
		try {
			JSONObject data = obj.getJSONObject(NDJSONTokens.EdgeTokens.DATA);
			
			Map<String, Object> params = new HashMap<String, Object>();
			
			String fromHGid = parseHGid(source, data.get(NDJSONTokens.EdgeTokens.FROM).toString());
			String toHGid = parseHGid(source, data.get(NDJSONTokens.EdgeTokens.TO).toString());
	
			params.put("fromParam", fromHGid);
			params.put("toParam", toHGid);
			params.put("typeParam", (String) data.get(NDJSONTokens.EdgeTokens.TYPE));
		
			// Query 1: Get and verify Vertex OUT
			ResultSet r = client.submitAsync("g.V().has('hgid', fromParam)", params).get();
			Vertex outVertexID = verifyVertex(r, fromHGid);
			
			// Query 2: Get and verify Vertex IN
			ResultSet r2 = client.submitAsync("g.V().has('hgid', toParam)", params).get();
			Vertex inVertexID = verifyVertex(r2, fromHGid);
			
			params.put("outVertexIDparam", outVertexID);
			params.put("inVertexIDparam", inVertexID);
			params.put("sourceParam", source);
			
			// Query 3: Create edge between Vertex OUT to IN
			ResultSet r3 = client.submitAsync("outVertexIDparam.attach(g).addEdge(typeParam, inVertexIDparam.attach(g), 'source', sourceParam)", params).get();
			for (Iterator<Result> i = r3.iterator(); i.hasNext(); ) {
				System.out.println("Got result: " + i.next());
			}
		} catch (JSONException e) {
			throw new IOException("Edge token(s) missing (data / from / to / type).");
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	private static Vertex verifyVertex(ResultSet r, String hgID) throws IOException {
		Iterator<Result> i = r.iterator();
		if (!i.hasNext()) throw new IOException ("Vertex with hgID '" + hgID + "' not found in graph.");
		Vertex v = i.next().getVertex();
		if (i.hasNext()) throw new IOException ("Multiple vertices with hgID '" + hgID + "' found in graph.");
		return v;
	}

	private static String parseHGid (String source, String id) {
		if (isNumeric(id)) {
			return source + "/" + id;
		} else {
			return id;
		}
	}
	
	public static void deleteVertex(JSONObject obj, String source, Client client) throws IOException {
		throw new IOException("Delete vertex command not yet implemented.");
	}
	
	public static void deleteEdge(JSONObject obj, String source, Client client) throws IOException {
		throw new IOException("Delete edge command not yet implemented.");
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