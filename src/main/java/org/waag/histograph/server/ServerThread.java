package org.waag.histograph.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONException;
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
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;

import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.servlet.ServletHandler;

public class ServerThread implements Runnable {

	static final String TRAVERSAL_PATH = "/traversal";
	
	static GraphDatabaseService db;
	static Configuration config;
	static String version;
	
	public ServerThread(GraphDatabaseService db, Configuration config, String version) {
		ServerThread.db = db;
		ServerThread.config = config;
		ServerThread.version = version;
	}

    public void run() {
        Server server = new Server(config.TRAVERSAL_PORT);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(BaseServlet.class, "/");
        handler.addServletWithMapping(TraversalServlet.class, TRAVERSAL_PATH);
        server.setHandler(handler);    
    	
        try {
			server.start();
	        System.out.println("Traversal server listening on http://localhost:" + config.TRAVERSAL_PORT + "/");
	        server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    @SuppressWarnings("serial")
	public static class BaseServlet extends HttpServlet {
        
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();
            
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("name", "histograph");
            jsonResponse.put("version", version);
            jsonResponse.put("message", "This is the Histograph Traversal API. Send POST requests with body '{hgids: [hgid1, hgid2, ...]}' to " + TRAVERSAL_PATH);
            
            out.println(jsonResponse);
        }
    }
    
    @SuppressWarnings("serial")
	public static class TraversalServlet extends HttpServlet {
        
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();
            
            out.println(errorResponse("The traversal API expects POST requests."));
        }
        
        protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            PrintWriter out = response.getWriter();
        	StringBuffer jb = new StringBuffer();
        	String line = null;
        	try {
        		BufferedReader reader = request.getReader();
        		while ((line = reader.readLine()) != null) jb.append(line);
        	} catch (Exception e) {	  
        		out.println(errorResponse("Error while parsing the POST request: " + e.getMessage()));
        	}
            
            try {
            	JSONObject jsonObj = new JSONObject(jb.toString());
            	JSONArray array = jsonObj.getJSONArray("hgids");
            	
            	String[] hgids = new String[array.length()];
            	for (int i=0; i<array.length(); i++) {
            		hgids[i] = array.getString(i);
            	}
            	
            	out.println(traverse(hgids));
            	
            } catch (JSONException e) {
            	out.println(errorResponse("Invalid JSON received."));
            }
        }
        
        public String errorResponse (String errorMsg) {
        	JSONObject out = new JSONObject();
        	out.put("message", errorMsg);
        	out.put("status", "error");
        	return out.toString();
        }
        
        public String traverse (String[] hgids) {
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
    					
	    				Node startNode = GraphMethods.getNode(db, hgid);
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
		    		    	Node node = GraphMethods.getNode(db, entry.getKey());
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
    		} catch (IOException e) {
    			return errorResponse(e.getMessage());
    		}
    		response.put("features", features);
        	return response.toString();
    	}
    }
}