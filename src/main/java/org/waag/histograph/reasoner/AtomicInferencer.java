package org.waag.histograph.reasoner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.util.GraphMethods;

public class AtomicInferencer {

	private GraphDatabaseService db;
	private static GraphMethods graphMethods;
	
	public AtomicInferencer (GraphDatabaseService db, ExecutionEngine engine) {
		this.db = db;
		graphMethods = new GraphMethods(db, engine);
	}
	
	public void inferAtomic(Map<String, String> params, Node fromNode, Node toNode) throws IOException {
		String label;
		try {	
			label = params.get(NDJSONTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = GraphTypes.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			inferAtomicEdges(params, atomicLabels, fromNode, toNode);
		}
	}
	
	public void removeInferredAtomic(Map<String, String> params) throws IOException {
		String label;
		try {
			label = params.get(NDJSONTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = GraphTypes.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			removeInferredAtomicEdges(params, atomicLabels);
		}
	}
	
	private void removeInferredAtomicEdges(Map<String, String> params, String[] labels) throws IOException {
		HashMap<String, String> inferredEdgeParams = new HashMap<String, String>(params);

		for (String label : labels) {
			inferredEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, label);
			
			Relationship rel = graphMethods.getEdge(inferredEdgeParams);
			if (rel == null) continue;
			
			if (atomicEdgeCanBeRemoved(inferredEdgeParams)) {
				try (Transaction tx = db.beginTx(); ) {
					rel.delete();
					tx.success();
				}
			}
		}
	}
	
	private boolean atomicEdgeCanBeRemoved(HashMap<String, String> atomicEdgeParams) throws IOException {
		String edgeLabel = atomicEdgeParams.get(NDJSONTokens.RelationTokens.LABEL);
		String[] primaryRels = GraphTypes.getPrimaryRelationsFromAtomic(edgeLabel);
		
		for (String primaryRel : primaryRels) {
			HashMap<String, String> primaryEdgeParams = new HashMap<String, String>(atomicEdgeParams);
			primaryEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, primaryRel);

			if (graphMethods.edgeExists(primaryEdgeParams)) return false;
		}
		return true;
	}

	private void inferAtomicEdges(Map<String, String> params, String[] labels, Node fromNode, Node toNode) throws IOException {
		HashMap<String, String> inferredEdgeParams = new HashMap<String, String>(params);
		for (String label : labels) {
			inferredEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, label);
			inferredEdgeParams.put(NDJSONTokens.General.LAYER, "inferred_from_" + params.get(NDJSONTokens.General.LAYER));
			
			// Take next label if edge already exists
			if (graphMethods.edgeExists(inferredEdgeParams)) continue;
			
			// Create edge between vertices
			try (Transaction tx = db.beginTx(); ) {
				Relationship rel = fromNode.createRelationshipTo(toNode, GraphTypes.RelationType.fromLabel(inferredEdgeParams.get(NDJSONTokens.RelationTokens.LABEL)));
				rel.setProperty(NDJSONTokens.General.LAYER, inferredEdgeParams.get(NDJSONTokens.General.LAYER));
				tx.success();
			}
		}
	}
}
