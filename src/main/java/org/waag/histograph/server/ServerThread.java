package org.waag.histograph.server;

import java.io.IOException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.waag.histograph.graph.GraphMethods;
import org.waag.histograph.reasoner.ReasoningDefinitions;

public class ServerThread implements Runnable {

	GraphDatabaseService db;
	
	public ServerThread(GraphDatabaseService db) {
		this.db = db;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Ik doe nu BFS!");
		
		Node startNode;
		try (Transaction tx = db.beginTx()) {
			startNode = GraphMethods.getVertex(db, "atlas-verstedelijking/Alkmaar_2010");
			
			TraversalDescription td = db.traversalDescription()
		            .breadthFirst()
		            .relationships(ReasoningDefinitions.RelationType.CONCEPTIDENTICAL, Direction.BOTH )
		            .evaluator( Evaluators.excludeStartPosition() );
		
			Traverser pitTraverser =  td.traverse(startNode);
			
			
		    int numberOfPiTs = 0;
		    String output = startNode.getProperty("name") + "'s buren:\n";
		    for (Path friendPath : pitTraverser) {
		        output += "At depth " + friendPath.length() + " => "
		                  + friendPath.endNode()
		                          .getProperty( "name" ) + "\n";
		        numberOfPiTs++;
		    }
		    output += "Number of PiTs found: " + numberOfPiTs + "\n";
			System.out.println(output);
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
