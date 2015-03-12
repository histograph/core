package org.waag.histograph.reasoner;

import java.io.IOException;
import java.util.Map;

import org.json.JSONException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.waag.histograph.graph.GraphMethods;
import org.waag.histograph.reasoner.ReasoningDefinitions.RelationType;
import org.waag.histograph.util.HistographTokens;

public class AtomicInferencer {

	public static void inferAtomic(GraphDatabaseService db, Relationship[] relationships) throws IOException {
		for (Relationship r : relationships) {
			String label = null;
			try (Transaction tx = db.beginTx()) {
				label = RelationType.fromRelationshipType(r.getType()).getLabel();
			}
			String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
			if (atomicLabels == null) {
				System.out.println("No atomic relations associated with label " + label);
				return;
			} else {
				String source = null;
				Node fromNode = null;
				Node toNode = null;
				try (Transaction tx = db.beginTx()) {
					source = r.getProperty(HistographTokens.General.SOURCE).toString();
					fromNode = r.getStartNode();
					toNode = r.getEndNode();
				}
				inferAtomicRelations(db, atomicLabels, fromNode, toNode, source);
			}
		}
	}
	
	public static void removeInferredAtomic(GraphDatabaseService db, Map<String, String> params) throws IOException {
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
			removeInferredAtomicRelations(db, params, atomicLabels);
		}
	}
	
	private static void removeInferredAtomicRelations(GraphDatabaseService db, Map<String, String> params, String[] labels) throws IOException {		
		Node[] fromNodes = GraphMethods.getNodesFromParams(db, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = GraphMethods.getNodesFromParams(db, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.");
		if (toNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.");

		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				for (String label : labels) {
					RelationType relType = RelationType.fromLabel(label);
					
					Relationship rel = GraphMethods.getRelation(db, fromNode, toNode, relType);
					if (rel == null) continue;
					
					if (atomicRelationCanBeRemoved(db, fromNode, toNode, relType)) {
						try (Transaction tx = db.beginTx()) {
							rel.delete();
							tx.success();
						}
					}
				}
			}
		}
	}
	
	private static boolean atomicRelationCanBeRemoved(GraphDatabaseService db, Node fromNode, Node toNode, RelationType relType) throws IOException {
		String relationLabel = relType.getLabel();
		String[] primaryRels = ReasoningDefinitions.getPrimaryRelationsFromAtomic(relationLabel);
		
		for (String primaryRel : primaryRels) {
			RelationType primaryRelType = RelationType.fromLabel(primaryRel);
			if (GraphMethods.relationExists(db, fromNode, toNode, primaryRelType)) return false;
		}
		return true;
	}

	private static void inferAtomicRelations(GraphDatabaseService db, String[] labels, Node fromNode, Node toNode, String originalSource) throws IOException {
		String inferredSource = "inferred_from_" + originalSource;
		for (String label : labels) {
			// Take next label if relation already exists
			if (GraphMethods.relationExists(db, fromNode, toNode, RelationType.fromLabel(label))) continue;
			
			// Create relation between nodes
			try (Transaction tx = db.beginTx()) {
				Relationship rel = fromNode.createRelationshipTo(toNode, RelationType.fromLabel(label));
				rel.setProperty(HistographTokens.General.SOURCE, inferredSource);
				tx.success();
			}
		}
	}
}