package org.waag.histograph.reasoner;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.waag.histograph.graph.GraphMethods;
import org.waag.histograph.util.HistographTokens;

/**
 * A class containing a method for inferring transitive relationships in a graph.
 * For now, only sameAs transitivity is inferred, i.e. (n1) --[SAMEAS]-- (n2) --[REL]--) (n3) should infer (n1) --[REL]--) (n3)
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class TransitiveInferencer {
	
	/**
	 * Infers all possible transitive relationships in a graph. For now, only sameAs transitivity is implemented.
	 * This method keeps running until no new relationships can be derived.
	 * @param db The neo4j GraphDatabaseService object.
	 * @return The number of inferred relationships.
	 */
	public static int inferTransitiveRelations (GraphDatabaseService db) {	
		int roundCount, inferredCount = 0;
		GlobalGraphOperations graphOps = GlobalGraphOperations.at(db);
		do {
			roundCount = 0;
			try (Transaction tx = db.beginTx();) {
				for (Relationship r : graphOps.getAllRelationships()) {
					if (r.isType(ReasoningDefinitions.RelationType.SAMEAS)) {
						Node n = r.getStartNode();
						Node n2 = r.getEndNode();
						
						roundCount += inferSameAsTransitivity(db, n, n2);
						roundCount += inferSameAsTransitivity(db, n2, n);
					}
				}
				inferredCount += roundCount;
				tx.success();
			}
		} while (roundCount > 0);
		return inferredCount;
	}
	
	private static int inferSameAsTransitivity(GraphDatabaseService db, Node n1, Node n2) {
		String layer = "inferredTransitiveSameAsRelation";
		int inferred = 0;
		// (n1) --[SAMEAS]-- (n2) --[REL]--> (n3) should infer (n1) --[REL]--> (n3)
		for (Relationship r : n2.getRelationships(Direction.OUTGOING)) {
			Node n3 = r.getOtherNode(n2);
			if (!n1.equals(n3)) {
				RelationshipType type = r.getType();
				if (!GraphMethods.relationExists(db, n1, n3, type)) {
					Relationship rel = n1.createRelationshipTo(n3, type);
					rel.setProperty(HistographTokens.General.SOURCE, layer);
					inferred++;
				}
			}
		}
		// (n1) --[SAMEAS]-- (n2) <--[REL]-- (n3) should infer (n1) <--[REL]-- (n3)
		for (Relationship r : n2.getRelationships(Direction.INCOMING)) {
			Node n3 = r.getOtherNode(n2);
			if (!n1.equals(n3)) {
				RelationshipType type = r.getType();
				if (!GraphMethods.relationExists(db, n3, n1, type)) {
					Relationship rel = n3.createRelationshipTo(n1, type);
					rel.setProperty(HistographTokens.General.SOURCE, layer);
					inferred++;
				}
			}
		}
		return inferred;
	}
}