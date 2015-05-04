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
import org.waag.histograph.util.HistographTokens.PITIdentifyingMethod;

/**
 * A class containing methods for inferring atomic relationships from existing relationships.
 * The atomic relationships are defined at http://histograph.io/concepts/ and written out in the
 * {@link ReasoningDefinitions} class.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class AtomicInferencer {

	/**
	 * Infers all atomic relationships associated with a provided array of relationships present in the graph.
	 * @param db The Neo4j GraphDatabaseService object
	 * @param relationships An array of relationships from which the atomic relationships should be derived.
	 */
	public static void inferAtomic(GraphDatabaseService db, Relationship[] relationships) {
		for (Relationship r : relationships) {
			String label = null;
			try (Transaction tx = db.beginTx()) {
				label = RelationType.fromRelationshipType(r.getType()).toString();
			}
			String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
			if (atomicLabels == null) return;
			
			String source = null;
			Node fromNode = null;
			PITIdentifyingMethod fromIdMethod = null;
			Node toNode = null;
			PITIdentifyingMethod toIdMethod = null;
			
			try (Transaction tx = db.beginTx()) {
				source = r.getProperty(HistographTokens.General.SOURCEID).toString();
				fromNode = r.getStartNode();
				fromIdMethod = PITIdentifyingMethod.valueOf(r.getProperty(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD).toString());
				toNode = r.getEndNode();
				toIdMethod = PITIdentifyingMethod.valueOf(r.getProperty(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD).toString());
			}
			inferAtomicRelations(db, atomicLabels, fromNode, fromIdMethod, toNode, toIdMethod, source);	
		}
	}
	
	private static void inferAtomicRelations(GraphDatabaseService db, String[] labels, Node fromNode, PITIdentifyingMethod fromIdMethod, Node toNode, PITIdentifyingMethod toIdMethod, String originalSource) {
		String inferredSource = "inferred_from_" + originalSource;
		for (String label : labels) {
			// Take next label if relation already exists
			if (GraphMethods.relationExists(db, fromNode, fromIdMethod, toNode, toIdMethod, RelationType.fromLabel(label), inferredSource)) continue;
			
			// Create relation between nodes
			try (Transaction tx = db.beginTx()) {
				Relationship rel = fromNode.createRelationshipTo(toNode, RelationType.fromLabel(label));
				rel.setProperty(HistographTokens.General.SOURCEID, inferredSource);
				tx.success();
			}
		}
	}
	
	/**
	 * Remove inferred atomic relationships inferred from a set of relationship parameters.
	 * @param db The Neo4j GraphDatabaseService object
	 * @param params Map containing the relationship parameters. Typically created by the {@link org.waag.histograph.util.InputReader} class.
	 * @throws IOException Thrown when a relationship label is missing in the parameters, or when no nodes
	 * are found based on the provided parameters.
	 */
	public static void removeInferredAtomic(GraphDatabaseService db, Map<String, String> params) throws IOException {
		String label;
		try {
			label = params.get(HistographTokens.RelationTokens.LABEL);
		} catch (JSONException e) {
			throw new IOException ("Relation label missing.");
		}
		
		String[] atomicLabels = ReasoningDefinitions.getAtomicRelationsFromLabel(label);
		if (atomicLabels == null) return;
		removeInferredAtomicRelations(db, params, atomicLabels);
	}
	
	private static void removeInferredAtomicRelations(GraphDatabaseService db, Map<String, String> params, String[] labels) throws IOException {
		String inferredSource = "inferred_from_" + params.get(HistographTokens.General.SOURCEID);
		PITIdentifyingMethod fromIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.FROM_IDENTIFYING_METHOD));
		PITIdentifyingMethod toIdMethod = PITIdentifyingMethod.valueOf(params.get(HistographTokens.RelationTokens.TO_IDENTIFYING_METHOD));

		Node[] fromNodes = GraphMethods.getNodesByIdMethod(db, fromIdMethod, params.get(HistographTokens.RelationTokens.FROM));
		Node[] toNodes = GraphMethods.getNodesByIdMethod(db, toIdMethod, params.get(HistographTokens.RelationTokens.TO));
		
		if (fromNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.FROM) + "' found in graph.");
		if (toNodes == null) throw new IOException("No nodes with " + HistographTokens.General.HGID + "/" + HistographTokens.PITTokens.URI + " '" + params.get(HistographTokens.RelationTokens.TO) + "' found in graph.");

		for (Node fromNode : fromNodes) {
			for (Node toNode : toNodes) {
				for (String label : labels) {
					RelationType relType = RelationType.fromLabel(label);
					
					Relationship rel = GraphMethods.getRelation(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, inferredSource);
					if (rel == null) continue;
					
					if (atomicRelationCanBeRemoved(db, fromNode, fromIdMethod, toNode, toIdMethod, relType, inferredSource)) {
						try (Transaction tx = db.beginTx()) {
							rel.delete();
							tx.success();
						}
					}
				}
			}
		}
	}
	
	private static boolean atomicRelationCanBeRemoved(GraphDatabaseService db, Node fromNode, PITIdentifyingMethod fromIdMethod, Node toNode, PITIdentifyingMethod toIdMethod, RelationType relType, String inferredSource) throws IOException {
		String relationLabel = relType.toString();
		String[] primaryRels = ReasoningDefinitions.getPrimaryRelationsFromAtomic(relationLabel);
		
		for (String primaryRel : primaryRels) {
			RelationType primaryRelType = RelationType.fromLabel(primaryRel);
			if (GraphMethods.relationExists(db, fromNode, fromIdMethod, toNode, toIdMethod, primaryRelType, inferredSource)) return false;
		}
		return true;
	}
}