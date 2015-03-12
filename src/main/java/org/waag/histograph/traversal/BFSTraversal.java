package org.waag.histograph.traversal;

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
import org.waag.histograph.graph.GraphMethods;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.util.HistographTokens;

public class BFSTraversal {

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
					JSONArray relations = new JSONArray();
					
					JSONObject geometryObj = new JSONObject();
					JSONArray geometryArr = new JSONArray();
					
    				Node startNode = GraphMethods.getNodeByHgid(db, hgid);
	    			if (startNode == null) {
	    				response.append("hgids_not_found", hgid);
	    				continue;
	    			}
	    			
					Map<String, Integer> nRelsMap = new HashMap<String, Integer>();
					Set<Relationship> relSet = new HashSet<Relationship>();
					
					nRelsMap.put(hgid, startNode.getDegree());
	    			hgidsDone.add(hgid);
	    			
	    			TraversalDescription td = db.traversalDescription()
	    		            .breadthFirst()
	    		            .relationships(ReasoningDefinitions.RelationType.CONCEPTIDENTICAL, Direction.BOTH)
	    		            .evaluator(Evaluators.excludeStartPosition());
	    		
	    			Traverser pitTraverser =  td.traverse(startNode);
	    					    			
	    		    for (Path path : pitTraverser) {
	    		    	Node endNode = path.endNode();
	    		    	String hgidFound = endNode.getProperty(HistographTokens.General.HGID).toString();
	    		    	
	    		    	if (!hgidsDone.contains(hgidFound)) {
	    		    		nRelsMap.put(hgidFound, endNode.getDegree());
	    		    		hgidsDone.add(hgidFound);
	    		    	}
	    		    }
	    		    
	    		    // Adding relationships to Set filters out duplicates
	    		    for (Relationship r : pitTraverser.relationships()) {
	    		    	relSet.add(r);
	    		    }
	    		    
	    		    ValueComparator comparator = new ValueComparator(nRelsMap);
	    		    TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(comparator);
	    		    sortedMap.putAll(nRelsMap);
	    		    int pitIndex = 0;
					int geometryIndex = 0;
					boolean pitsPresentWithGeometry = false;
	    		    
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
    		    		
    		    		pits.put(pitIndex, pit);
    		    		pitIndex ++;
	    		    }
	    		    
	    		    if (pitsPresentWithGeometry) { // Only add PIT group if at least one geometry present
		    		    for (Relationship r : relSet) {
		    		    	JSONObject relObj = new JSONObject();
		    		    	relObj.put("from", r.getStartNode().getProperty(HistographTokens.General.HGID));
		    		    	relObj.put("to", r.getEndNode().getProperty(HistographTokens.General.HGID));
		    		    	relations.put(relObj);
		    		    }
		    		    
		    		    geometryObj.put("type", "GeometryCollection");
		    		    geometryObj.put("geometries", geometryArr);
		    		    
		    		    properties.put("pits", pits);
		    		    properties.put("relations", relations);
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