package org.waag.histograph.server;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.waag.histograph.graph.GraphMethods;
import org.waag.histograph.reasoner.ReasoningDefinitions;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("resource")
public class Resource {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return "Got it!";
    }
    
//	public String bfsTest() {
//		System.out.println("Ik doe nu BFS!");
//		
//		Node startNode;
//		try (Transaction tx = db.beginTx()) {
//			startNode = GraphMethods.getNode(db, "atlas-verstedelijking/Alkmaar_2010");
//			
//			TraversalDescription td = db.traversalDescription()
//		            .breadthFirst()
//		            .relationships(ReasoningDefinitions.RelationType.CONCEPTIDENTICAL, Direction.BOTH )
//		            .evaluator( Evaluators.excludeStartPosition() );
//		
//			Traverser pitTraverser =  td.traverse(startNode);
//			
//			
//		    int numberOfPiTs = 0;
//		    for (Path friendPath : pitTraverser) {
////		        output += "At depth " + friendPath.length() + " => "
////		                  + friendPath.endNode()
////		                          .getProperty( "name" ) + "\n";
//		        numberOfPiTs++;
//		    }
//
//			return "Number of PiTs found: " + numberOfPiTs + "\n";
//		
//		} catch (IOException e) {
//			e.printStackTrace();
//			return null;
//		} 
//	}   
}