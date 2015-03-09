package org.waag.histograph.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
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
import org.waag.histograph.util.HistographTokens;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;

import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.servlet.ServletHandler;

public class ServerThread implements Runnable {

	static GraphDatabaseService db;
	static final int port = 13782;
	
	public ServerThread(GraphDatabaseService db) {
		ServerThread.db = db;
	}

    public void run() {
        Server server = new Server(port);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(TraversalServlet.class, "/traversal"); //Set the servlet to run.
        handler.addServletWithMapping(ApiServlet.class, "/");
        server.setHandler(handler);    
    	
        try {
			server.start();
	        System.out.println("Traversal server listening on http://localhost:" + port + "/");
	        server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    @SuppressWarnings("serial")
	public static class ApiServlet extends HttpServlet {
        
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();
            out.println("<h1>Hee hallo!</h1>");
            
            Enumeration<String> e = request.getParameterNames();
            for (; e.hasMoreElements(); ) {
            	String param = e.nextElement();
            	out.println("<br>Parameter found: " + param + " with value " + request.getParameter(param));
            }
        }
    }
    
    @SuppressWarnings("serial")
	public static class TraversalServlet extends HttpServlet {
        
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();
            out.println("<h1>GET request!</h1>");
            
            Enumeration<String> e = request.getParameterNames();
            for (; e.hasMoreElements(); ) {
            	String param = e.nextElement();
            	out.println("<br>Parameter found: " + param + " with value " + request.getParameter(param));
            }
        }
        
        protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        	response.setContentType("application/json");
        	response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();

        	String input = request.getParameter("hgids");

            if (input == null) {
            	out.println(errorResponse("No hgid(s) given."));
            	return;
            }
            
            JSONObject jsonObj = null;
            
            try {
            	jsonObj = new JSONObject(input);
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
    					JSONArray geometries = new JSONArray();
    					
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
		    		    
		    		    for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
		    		    	JSONObject pit = new JSONObject();
		    		    	Node node = GraphMethods.getNode(db, entry.getKey());
		    		    	
	    		    		for (String key : node.getPropertyKeys()) {
	    		    			if (key.equals(HistographTokens.PITTokens.GEOMETRY)) {
	    		    				pit.put("geometryIndex", geometryIndex);
	    		    				geometries.put(geometryIndex, new JSONObject(node.getProperty(key).toString()));
	    		    				geometryIndex++;
	    		    			} else if (key.equals(HistographTokens.PITTokens.TYPE)) {
	    		    				properties.put(HistographTokens.PITTokens.TYPE, node.getProperty(key));
	    		    			} else {
	    		    				pit.put(key, node.getProperty(key));
	    		    			}
	    		    		}
	    		    		
	    		    		pits.put(pitIndex, pit);
	    		    		pitIndex ++;
		    		    }
		    		    
		    		    for (Relationship r : relSet) {
		    		    	JSONObject relObj = new JSONObject();
		    		    	relObj.put("from", r.getStartNode().getProperty(HistographTokens.General.HGID));
		    		    	relObj.put("to", r.getEndNode().getProperty(HistographTokens.General.HGID));
		    		    	relations.put(relObj);
		    		    }
		    		    
		    		    properties.put("pits", pits);
		    		    properties.put("relations", relations);
		    		    feature.put("properties", properties);
		    		    feature.put("geometry", geometries);
	    				features.put(feature);
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