package org.waag.histograph.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.driver.ResultSet;

public class ClientMethods {
	
	public static ResultSet submitQuery(Client client, String query, Map<String, Object> params) throws IOException {
		try {
			return client.submitAsync(query, params).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException("Exception while executing remote query:", e);
		}
	}
	
	public static boolean vertexExists(Client client, String hgID) throws IOException {
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', '" + hgID + "')", null);
		Iterator<Result> i = r.iterator();
		if (!i.hasNext()) return false;
		i.next();
		if (i.hasNext()) throw new IOException ("Multiple vertices with hgID '" + hgID + "' found in graph.");
		return true;
	}
	
	public static boolean edgeExists(Client client, Map<String, Object> params) throws IOException {
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x')", params);
		Iterator<Result> i = r.iterator();
		String edgeName = params.get("fromParam").toString() + " --" + params.get("labelParam").toString() + "--> " + params.get("toParam");
		if (!i.hasNext()) return false;
		i.next();
		if (i.hasNext()) throw new IOException ("Multiple edges '" + edgeName + "' found in graph.");
		return true;
	}
	
	public static boolean vertexAbsent(Client client, String hgID) throws IOException {
		ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', '" + hgID + "')", null);
		Iterator<Result> i = r.iterator();
		if (i.hasNext()) {
			return false;
		} else {
			return true;
		}
	}
	
	public static boolean edgeAbsent(Client client, Map<String, Object> params) throws IOException {
		ResultSet r = submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).inV().has('hgid', toParam)", params);
		Iterator<Result> i = r.iterator();
		if (i.hasNext()) {
			return false;
		} else {
			return true;
		}
	}
	
	// Neo4j only handles removals asynchronously, allowing for concurrency issues. This method is to circumvent this problem.
	public static void waitForEdgeAbsent(Client client, Map<String, Object> params) throws IOException {
		while (edgeExists(client, params)) {}
		return;
	}
	
}