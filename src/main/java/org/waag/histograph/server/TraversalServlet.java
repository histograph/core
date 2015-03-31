package org.waag.histograph.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.waag.histograph.graph.BFSTraversal;

@SuppressWarnings("serial")
/**
 * A servlet class handling GET requests with an error message and POST requests by calling the BFSTraversal class.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class TraversalServlet extends HttpServlet {
	
	private GraphDatabaseService db;
	
	public TraversalServlet (GraphDatabaseService db) {
		this.db = db;
	}
    
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        response.setCharacterEncoding("UTF-8");
        response.setStatus(HttpServletResponse.SC_OK);
        PrintWriter out = response.getWriter();
        
        out.println(errorResponse("The traversal API expects POST requests."));
    }
    
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
    	response.setStatus(HttpServletResponse.SC_OK);
    	
        PrintWriter out = response.getWriter();
    	StringBuffer jb = new StringBuffer();
    	
    	// Read raw input
    	String line = null;
    	try {
    		BufferedReader reader = request.getReader();
    		while ((line = reader.readLine()) != null) jb.append(line);
    	} catch (Exception e) {	  
    		out.println(errorResponse("Error while parsing POST request: " + e.getMessage()));
    		return;
    	}
        
    	// Extract JSON body and perform traversal
        try {
        	JSONObject jsonObj = new JSONObject(jb.toString());
        	JSONArray array = jsonObj.getJSONArray("hgids");
        	
        	String[] hgids = new String[array.length()];
        	for (int i=0; i<array.length(); i++) {
        		hgids[i] = array.getString(i);
        	}
        	
        	JSONObject bfsTraversal = BFSTraversal.traverse(db, hgids);
        	out.println(bfsTraversal.toString());
        	
        } catch (JSONException e) {
        	out.println(errorResponse("Invalid JSON received" + e.getMessage()));
        	return;
        } catch (IOException e) {
        	out.println(errorResponse(e.getMessage()));
        	return;
        }
    }
    
    private String errorResponse (String errorMsg) {
    	JSONObject out = new JSONObject();
    	out.put("message", errorMsg);
    	out.put("status", "error");
    	return out.toString();
    }
}