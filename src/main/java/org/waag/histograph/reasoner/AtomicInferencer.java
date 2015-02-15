package org.waag.histograph.reasoner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.waag.histograph.util.ClientMethods;

import com.tinkerpop.gremlin.driver.Client;
import com.tinkerpop.gremlin.driver.Result;
import com.tinkerpop.gremlin.driver.ResultSet;

public class AtomicInferencer {
	
	public static void inferAtomic(Client client, Map<String, Object> params) throws IOException {
		String label;
		try {	
			label = params.get("labelParam").toString();
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = OntologyTokens.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			inferAtomicEdges(client, params, atomicLabels);
		}
	}
	
	public static void removeInferredAtomic(Client client, Map<String, Object> params) throws IOException {
		String label;
		try {
			label = params.get("labelParam").toString();
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = OntologyTokens.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			removeInferredAtomicEdges(client, params, atomicLabels);
		}
	}
	
	private static void removeInferredAtomicEdges(Client client, Map<String, Object> params, String[] labels) throws IOException {
		HashMap<String, Object> inferredEdgeParams = new HashMap<String, Object>(params);
		int removed = 0;
		for (String label : labels) {
			inferredEdgeParams.put("labelParam", label);
			if (!ClientMethods.edgeExists(client, inferredEdgeParams)) {
				break;
			}
			
			if (atomicEdgeCanBeRemoved(client, inferredEdgeParams)) {
				ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).outE().has(label, labelParam).has('layer', layerParam).as('x').inV().has('hgid', toParam).back('x').remove()", inferredEdgeParams);
				removed ++;
			}
		}
		
		if (removed != 1) {
			System.out.println("Removed " + removed + " atomic relation edges.");
		} else {
			System.out.println("Removed 1 atomic relation edge.");
		}
	}
	
	private static boolean atomicEdgeCanBeRemoved(Client client, HashMap<String, Object> inferredEdgeParams) throws IOException {
		String edgeLabel = inferredEdgeParams.get("labelParam").toString();
		String[] primaryRels = OntologyTokens.getPrimaryRelationsFromAtomic(edgeLabel);
		
		for (String primaryRel : primaryRels) {
			HashMap<String, Object> primaryEdgeParams = new HashMap<String, Object>(inferredEdgeParams);
			primaryEdgeParams.put("labelParam", primaryRel);
			if (ClientMethods.edgeExists(client, primaryEdgeParams)) {
				return false;
			}
		}
		return true;
	}

	private static void inferAtomicEdges(Client client, Map<String, Object> params, String[] labels) throws IOException {
		HashMap<String, Object> inferredEdgeParams = new HashMap<String, Object>(params);
		int inferred = 0;
		for (String label : labels) {
			inferredEdgeParams.put("labelParam", label);
			if (!ClientMethods.edgeAbsent(client, inferredEdgeParams)) break;

			ResultSet r = ClientMethods.submitQuery(client, "g.V().has('hgid', fromParam).next().addEdge(labelParam, g.V().has('hgid', toParam).next(), 'layer', layerParam)", inferredEdgeParams);
			for (Iterator<Result> i = r.iterator(); i.hasNext(); ) {
				System.out.println("Got result: " + i.next());
			}
			inferred ++;
		}
		
		if (inferred != 1) {
			System.out.println("Inferred " + inferred + " atomic relation edges.");
		} else {
			System.out.println("Inferred 1 atomic relation edge.");
		}
	}
}
