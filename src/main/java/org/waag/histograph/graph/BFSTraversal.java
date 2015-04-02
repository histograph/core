package org.waag.histograph.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.reasoner.ReasoningDefinitions.RelationType;
import org.waag.histograph.server.ServerThread;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.ValueComparator;

/**
 * A class with a method for breadth-first search traversal through a Neo4j graph. The response
 * is converted to JSON, which can be returned in the traversal API response in the {@link ServerThread} class.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class BFSTraversal {

	/**
	 * Performs the breadth-first search traversal through the graph and returns the response in
	 * JSON format. Only hga:Conceptidentical relationships are traversed in both directions.
	 * @param db The Neo4j GraphDatabaseService object
	 * @param hgids An array of hgids, typically the result of an Elasticsearch query, representing the
	 * start nodes of the traversal. If a start node B in the array is encountered during traversal of 
	 * start node A in the array, then node B is not taken as a start node later on.
	 * @return A JSON object with all traversals separately, ready to be returned by the Traversal API ({@link ServerThread}).
	 * @throws IOException Propagated exception for when multiple nodes with the same hgid are found. This should
	 * never happen because of the uniqueness constraint defined on the hgid property.
	 */
	public static JSONObject traverse (GraphDatabaseService db, String[] hgids) throws IOException {
    	ArrayList<String> hgidsDone = new ArrayList<String>();
    	JSONObject response = new JSONObject();
    	
    	response.put("type", "FeatureCollection");
    	JSONArray features = new JSONArray();
    	        	
		try (Transaction tx = db.beginTx()) {
			for (String hgid : hgids) {
				if (!hgidsDone.contains(hgid)) {
					
					JSONObject feature = new JSONObject();
					feature.put("type", "Feature");
					
					JSONObject properties = new JSONObject();
					JSONArray pits = new JSONArray();
					
					JSONObject geometryObj = new JSONObject();
					JSONArray geometryArr = new JSONArray();
					
    				Node startNode = GraphMethods.getNodeByHgid(db, hgid);
	    			if (startNode == null) {
	    				response.append("hgids_not_found", hgid);
	    				continue;
	    			}
	    			
	    			// Create a map with <Node, nRels> pairs to sort nodes by the number of relationships later on
					Map<String, Integer> nRelsMap = new HashMap<String, Integer>();
					Set<Relationship> relSet = new HashSet<Relationship>();
					
					nRelsMap.put(hgid, startNode.getDegree());
	    			hgidsDone.add(hgid);
	    			
	    			// We only traverse the SameHgConcept relationship
	    			TraversalDescription td = db.traversalDescription()
	    		            .breadthFirst()
	    		            .relationships(ReasoningDefinitions.RelationType.SAMEHGCONCEPT, Direction.BOTH)
	    		            .evaluator(Evaluators.excludeStartPosition());
	    		
	    			Traverser pitTraverser =  td.traverse(startNode);
	    			
	    			// Get all nodes found in each path, add them to the list if they weren't added before
	    		    for (Path path : pitTraverser) {
	    		    	Node endNode = path.endNode();
	    		    	String hgidFound = endNode.getProperty(HistographTokens.General.HGID).toString();
	    		    	
	    		    	if (!hgidsDone.contains(hgidFound)) {
	    		    		nRelsMap.put(hgidFound, endNode.getDegree());
	    		    		hgidsDone.add(hgidFound);
	    		    	}
	    		    }
	    		    
	    		    // Add relationships to a Set to filter out duplicates
	    		    for (Relationship r : pitTraverser.relationships()) {
	    		    	relSet.add(r);
	    		    }
	    		    
	    		    // Sort nodes by #relationships
	    		    ValueComparator comparator = new ValueComparator(nRelsMap);
	    		    TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(comparator);
	    		    sortedMap.putAll(nRelsMap);
	    		    
	    		    int pitIndex = 0;
					int geometryIndex = 0;
					boolean pitsPresentWithGeometry = false;
	    		    
					// For each node in the Set (= hgConcept), create JSON output
	    		    for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
	    		    	JSONObject pit = new JSONObject();
	    		    	Node node = GraphMethods.getNodeByHgid(db, entry.getKey());
	    		    	boolean hasGeometry = false;
	    		    	
    		    		for (String key : node.getPropertyKeys()) {
    		    			if (key.equals(HistographTokens.PITTokens.GEOMETRY)) {
    		    				pit.put("geometryIndex", geometryIndex);
    		    				geometryArr.put(geometryIndex, new JSONObject(node.getProperty(key).toString()));
    		    				hasGeometry = true;
    		    				pitsPresentWithGeometry = true;
    		    				geometryIndex++;
    		    			} else if (key.equals(HistographTokens.PITTokens.TYPE)) {
    		    				pit.put(HistographTokens.PITTokens.TYPE, node.getProperty(key));
    		    				properties.put(HistographTokens.PITTokens.TYPE, node.getProperty(key));
    		    			} else if (key.equals(HistographTokens.PITTokens.DATA)) {
    		    				pit.put(HistographTokens.PITTokens.DATA, new JSONObject(node.getProperty(key).toString()));
    		    			} else {
    		    				pit.put(key, node.getProperty(key));
    		    			}
    		    		}
    		    		
    		    		if (!hasGeometry) {
		    				pit.put("geometryIndex", -1);
    		    		}
    		    		
    		    		JSONObject pitRelations = new JSONObject();
    		    		
    		    		// Add all outgoing relationships to each PIT
    		    		for (Relationship r : relSet) {
    		    			if (r.getStartNode().equals(node)) {
    		    				String type = RelationType.fromRelationshipType(r.getType()).toString();
    		    				if (pitRelations.has(type)) {
    		    					pitRelations.getJSONArray(type).put(r.getEndNode().getProperty(HistographTokens.General.HGID));
    		    				} else {
    		    					JSONArray relArray = new JSONArray();
    		    					relArray.put(r.getEndNode().getProperty(HistographTokens.General.HGID));
    		    					pitRelations.put(type, relArray);
    		    				}
    		    			}
    		    		}
    		    		
    		    		if (pitRelations.length() > 0) {
    		    			pit.put("relations", pitRelations);
    		    		}
    		    		
    		    		pits.put(pitIndex, pit);
    		    		pitIndex ++;
	    		    }
	    		    
	    		    // Only add the HgConcept to the JSON output if at least one geometry is present
	    		    if (pitsPresentWithGeometry) {

		    		    geometryObj.put("type", "GeometryCollection");
		    		    geometryObj.put("geometries", geometryArr);
		    		    
		    		    properties.put("pits", pits);
		    		    feature.put("properties", properties);
		    		    feature.put("geometry", geometryObj);
	    				features.put(feature);
	    		    }
				}
			}
			response.put("features", features);
	    	return response;
		}
	}
}