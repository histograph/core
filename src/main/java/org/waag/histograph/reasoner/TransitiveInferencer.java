package org.waag.histograph.reasoner;

import java.io.IOException;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.util.GraphMethods;

public class TransitiveInferencer {
	
	private GraphDatabaseService db;
	private static GraphMethods graphMethods;
	private GlobalGraphOperations graphOps;
	
	public TransitiveInferencer (GraphDatabaseService db, ExecutionEngine engine) {
		this.db = db;
		graphMethods = new GraphMethods(db, engine);
		graphOps = GlobalGraphOperations.at(db);
	}
	
	public void inferTransitiveEdges () throws IOException {	
		int roundCount, inferredCount = 0;
		do {
			roundCount = 0;
			try (Transaction tx = db.beginTx();) {
				for (Relationship r : graphOps.getAllRelationships()) {
					if (r.isType(GraphDefinitions.RelationType.SAMEAS)) {
						Node n = r.getStartNode();
						Node n2 = r.getEndNode();
						
						roundCount += inferSameAsTransitivity(n, n2);
						roundCount += inferSameAsTransitivity(n2, n);
					}
				}
				inferredCount += roundCount;
				System.out.println("  Inferred " + roundCount + " transitive edges this round.");
				tx.success();
			}
		} while (roundCount > 0);
		System.out.println("Inferred " + inferredCount + " transitive edges.");
	}
	
	private int inferSameAsTransitivity(Node n1, Node n2) throws IOException {
		String layer = "inferredTransitiveSameAsEdge";
		int inferred = 0;
		// (n1) --[SAMEAS]-- (n2) --[REL]--> (n3) should infer (n1) --[REL]--> (n3)
		for (Relationship r : n2.getRelationships(Direction.OUTGOING)) {
			Node n3 = r.getOtherNode(n2);
			if (!n1.equals(n3)) {
				RelationshipType type = r.getType();
				if (!graphMethods.edgeExists(n1, n3, type)) {
					Relationship rel = n1.createRelationshipTo(n3, type);
					rel.setProperty(NDJSONTokens.General.LAYER, layer);
					inferred++;
				}
			}
		}
		// (n1) --[SAMEAS]-- (n2) <--[REL]-- (n3) should infer (n1) <--[REL]-- (n3)
		for (Relationship r : n2.getRelationships(Direction.INCOMING)) {
			Node n3 = r.getOtherNode(n2);
			if (!n1.equals(n3)) {
				RelationshipType type = r.getType();
				if (!graphMethods.edgeExists(n3, n1, type)) {
					n3.createRelationshipTo(n1, type);
					inferred++;
				}
			}
		}
		return inferred;
	}
}