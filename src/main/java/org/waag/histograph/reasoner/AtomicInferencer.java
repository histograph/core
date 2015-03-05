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
import org.waag.histograph.graph.GraphMethods;
import org.waag.histograph.queue.NDJSONTokens;

public class AtomicInferencer {

	public static void inferAtomic(GraphDatabaseService db, ExecutionEngine engine, 
								Map<String, String> params, Node fromNode, Node toNode) throws IOException {
		String label;
		try {	
			label = params.get(NDJSONTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			inferAtomicEdges(db, engine, params, atomicLabels, fromNode, toNode);
		}
	}
	
	public static void removeInferredAtomic(GraphDatabaseService db, ExecutionEngine engine, 
												Map<String, String> params) throws IOException {
		String label;
		try {
			label = params.get(NDJSONTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Label missing from edge.");
		}
		
		String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			removeInferredAtomicEdges(db, engine, params, atomicLabels);
		}
	}
	
	private static void removeInferredAtomicEdges(GraphDatabaseService db, ExecutionEngine engine,
									Map<String, String> params, String[] labels) throws IOException {
		HashMap<String, String> inferredEdgeParams = new HashMap<String, String>(params);

		for (String label : labels) {
			inferredEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, label);
			
			Relationship rel = GraphMethods.getEdge(db, engine, inferredEdgeParams);
			if (rel == null) continue;
			
			if (atomicEdgeCanBeRemoved(db, engine, inferredEdgeParams)) {
				try (Transaction tx = db.beginTx()) {
					rel.delete();
					tx.success();
				}
			}
		}
	}
	
	private static boolean atomicEdgeCanBeRemoved(GraphDatabaseService db, ExecutionEngine engine, 
								Map<String, String> atomicEdgeParams) throws IOException {
		String edgeLabel = atomicEdgeParams.get(NDJSONTokens.RelationTokens.LABEL);
		String[] primaryRels = ReasoningDefinitions.getPrimaryRelationsFromAtomic(edgeLabel);
		
		for (String primaryRel : primaryRels) {
			HashMap<String, String> primaryEdgeParams = new HashMap<String, String>(atomicEdgeParams);
			primaryEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, primaryRel);

			if (GraphMethods.edgeExists(db, engine, primaryEdgeParams)) return false;
		}
		return true;
	}

	private static void inferAtomicEdges(GraphDatabaseService db, ExecutionEngine engine, 
							Map<String, String> params, String[] labels, Node fromNode, Node toNode) throws IOException {
		HashMap<String, String> inferredEdgeParams = new HashMap<String, String>(params);
		for (String label : labels) {
			inferredEdgeParams.put(NDJSONTokens.RelationTokens.LABEL, label);
			inferredEdgeParams.put(NDJSONTokens.General.LAYER, "inferred_from_" + params.get(NDJSONTokens.General.LAYER));
			
			// Take next label if edge already exists
			if (GraphMethods.edgeExists(db, engine, inferredEdgeParams)) continue;
			
			// Create edge between vertices
			try (Transaction tx = db.beginTx()) {
				Relationship rel = fromNode.createRelationshipTo(toNode, ReasoningDefinitions.RelationType.fromLabel(inferredEdgeParams.get(NDJSONTokens.RelationTokens.LABEL)));
				rel.setProperty(NDJSONTokens.General.LAYER, inferredEdgeParams.get(NDJSONTokens.General.LAYER));
				tx.success();
			}
		}
	}
}