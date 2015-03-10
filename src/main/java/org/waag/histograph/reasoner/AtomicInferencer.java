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
import org.waag.histograph.util.HistographTokens;

public class AtomicInferencer {

	public static void inferAtomic(GraphDatabaseService db, ExecutionEngine engine, 
								Map<String, String> params, Node fromNode, Node toNode) throws IOException {
		String label;
		try {	
			label = params.get(HistographTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Relation label missing.");
		}
		
		String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			inferAtomicRelations(db, engine, params, atomicLabels, fromNode, toNode);
		}
	}
	
	public static void removeInferredAtomic(GraphDatabaseService db, ExecutionEngine engine, 
												Map<String, String> params) throws IOException {
		String label;
		try {
			label = params.get(HistographTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Relation label missing.");
		}
		
		String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) {
			System.out.println("No atomic relations associated with label " + label);
			return;
		} else {
			removeInferredAtomicRelations(db, engine, params, atomicLabels);
		}
	}
	
	private static void removeInferredAtomicRelations(GraphDatabaseService db, ExecutionEngine engine,
									Map<String, String> params, String[] labels) throws IOException {
		HashMap<String, String> inferredRelationParams = new HashMap<String, String>(params);

		for (String label : labels) {
			inferredRelationParams.put(HistographTokens.RelationTokens.LABEL, label);
			
			Relationship rel = GraphMethods.getRelation(db, engine, inferredRelationParams);
			if (rel == null) continue;
			
			if (atomicRelationCanBeRemoved(db, engine, inferredRelationParams)) {
				try (Transaction tx = db.beginTx()) {
					rel.delete();
					tx.success();
				}
			}
		}
	}
	
	private static boolean atomicRelationCanBeRemoved(GraphDatabaseService db, ExecutionEngine engine, 
								Map<String, String> atomicRelationParams) throws IOException {
		String relationLabel = atomicRelationParams.get(HistographTokens.RelationTokens.LABEL);
		String[] primaryRels = ReasoningDefinitions.getPrimaryRelationsFromAtomic(relationLabel);
		
		for (String primaryRel : primaryRels) {
			HashMap<String, String> primaryRelationParams = new HashMap<String, String>(atomicRelationParams);
			primaryRelationParams.put(HistographTokens.RelationTokens.LABEL, primaryRel);

			if (GraphMethods.relationExists(db, engine, primaryRelationParams)) return false;
		}
		return true;
	}

	private static void inferAtomicRelations(GraphDatabaseService db, ExecutionEngine engine, 
							Map<String, String> params, String[] labels, Node fromNode, Node toNode) throws IOException {
		HashMap<String, String> inferredRelationParams = new HashMap<String, String>(params);
		for (String label : labels) {
			inferredRelationParams.put(HistographTokens.RelationTokens.LABEL, label);
			inferredRelationParams.put(HistographTokens.General.SOURCE, "inferred_from_" + params.get(HistographTokens.General.SOURCE));
			
			// Take next label if relation already exists
			if (GraphMethods.relationExists(db, engine, inferredRelationParams)) continue;
			
			// Create relation between nodes
			try (Transaction tx = db.beginTx()) {
				Relationship rel = fromNode.createRelationshipTo(toNode, ReasoningDefinitions.RelationType.fromLabel(inferredRelationParams.get(HistographTokens.RelationTokens.LABEL)));
				rel.setProperty(HistographTokens.General.SOURCE, inferredRelationParams.get(HistographTokens.General.SOURCE));
				tx.success();
			}
		}
	}
}