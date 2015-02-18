package org.waag.histograph.reasoner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.util.CypherGraphMethods;

public class CypherAtomicInferencer {
	
	private GraphDatabaseService db;
	private static CypherGraphMethods graphMethods;
	
	public CypherAtomicInferencer (GraphDatabaseService db, ExecutionEngine engine) {
		this.db = db;
		graphMethods = new CypherGraphMethods(db, engine);
	}
	
	public void inferAtomic(Map<String, String> params) throws IOException {
		String label;
		try {	
			label = params.get(NDJSONTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = OntologyTokens.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			inferAtomicEdges(params, atomicLabels);
		}
	}
	
	public void removeInferredAtomic(Map<String, String> params) throws IOException {
		String label;
		try {
			label = params.get(NDJSONTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = OntologyTokens.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			removeInferredAtomicEdges(params, atomicLabels);
		}
	}
	
	private void removeInferredAtomicEdges(Map<String, String> params, String[] labels) throws IOException {
		HashMap<String, String> inferredEdgeParams = new HashMap<String, String>(params);
		int removed = 0;
		for (String label : labels) {
			inferredEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, label);
			
			Relationship rel = graphMethods.getEdge(inferredEdgeParams);
			if (rel == null) break;
			
			if (atomicEdgeCanBeRemoved(inferredEdgeParams)) {
				try (Transaction tx = db.beginTx(); ) {
					rel.delete();
					tx.success();
				}
				removed ++;
			}
		}
		
		if (removed != 1) {
			System.out.println("Removed " + removed + " atomic relation edges.");
		} else {
			System.out.println("Removed 1 atomic relation edge.");
		}
	}
	
	private static boolean atomicEdgeCanBeRemoved(HashMap<String, String> atomicEdgeParams) throws IOException {
		String edgeLabel = atomicEdgeParams.get(NDJSONTokens.RelationTokens.LABEL);
		String[] primaryRels = OntologyTokens.getPrimaryRelationsFromAtomic(edgeLabel);
		
		for (String primaryRel : primaryRels) {
			HashMap<String, String> primaryEdgeParams = new HashMap<String, String>(atomicEdgeParams);
			primaryEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, primaryRel);
			if (graphMethods.edgeExists(primaryEdgeParams)) {
				return false;
			}
		}
		return true;
	}

	private void inferAtomicEdges(Map<String, String> params, String[] labels) throws IOException {
		HashMap<String, String> inferredEdgeParams = new HashMap<String, String>(params);
		int inferred = 0;
		for (String label : labels) {
			inferredEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, label);
			
			// Break if edge already exists
			if (graphMethods.edgeExists(inferredEdgeParams)) break;

			// Get both vertices
			Node fromNode = graphMethods.getVertex(inferredEdgeParams.get(NDJSONTokens.RelationTokens.FROM));
			if (fromNode == null) throw new IOException("Vertex with hgID " + inferredEdgeParams.get(NDJSONTokens.RelationTokens.FROM) + " not found in graph.");
			
			Node toNode = graphMethods.getVertex(inferredEdgeParams.get(NDJSONTokens.RelationTokens.TO));
			if (toNode == null) throw new IOException("Vertex with hgID " + inferredEdgeParams.get(NDJSONTokens.RelationTokens.TO) + " not found in graph.");		
			
			// Create edge between vertices
			try (Transaction tx = db.beginTx(); ) {
				RelationshipType relType = DynamicRelationshipType.withName(inferredEdgeParams.get(NDJSONTokens.RelationTokens.LABEL));
				Relationship rel = fromNode.createRelationshipTo(toNode, relType);
				rel.setProperty(NDJSONTokens.General.LAYER, inferredEdgeParams.get(NDJSONTokens.General.LAYER));
				tx.success();
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
