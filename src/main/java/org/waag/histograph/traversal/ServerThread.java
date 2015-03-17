package org.waag.histograph.traversal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.waag.histograph.util.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;

import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.servlet.ServletHandler;

/**
 * A single thread class that acts as a server for the Traversal API.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class ServerThread implements Runnable {

	static final String TRAVERSAL_PATH = "/traversal";
	
	static GraphDatabaseService db;
	static Configuration config;
	static String version;
	
	/**
	 * Constructor of the thread.
	 * @param db The Neo4j graph object on which the operations should be performed.
	 * @param config The Configuration object in which the server's port number is specified.
	 * @param version The version of Histograph-Core.
	 */
	public ServerThread(GraphDatabaseService db, Configuration config, String version) {
		ServerThread.db = db;
		ServerThread.config = config;
		ServerThread.version = version;
	}

	/**
	 * The thread's main method. Starts listening on the port defined in the Configuration object provided in the 
	 * Thread's constructor and accepts traversal requests through POST requests at the traversal path.
	 */
    public void run() {
        Server server = new Server(config.TRAVERSAL_PORT);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(BaseServlet.class, "/");
        handler.addServletWithMapping(TraversalServlet.class, TRAVERSAL_PATH);
        server.setHandler(handler);    
    	
        try {
			server.start();
	        System.out.println("Traversal API server listening on http://localhost:" + config.TRAVERSAL_PORT + "/");
	        server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    @SuppressWarnings("serial")
    /**
     * A servlet class handling GET requests at the root path.
     * @author Rutger van Willigen
     * @author Bert Spaan
     */
	public static class BaseServlet extends HttpServlet {
        
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            PrintWriter out = response.getWriter();
            
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("name", "histograph");
            jsonResponse.put("version", version);
            jsonResponse.put("message", "This is the Histograph traversal API. Send POST requests with body '{hgids: [hgid1, hgid2, ...]}' to " + TRAVERSAL_PATH);
            
            out.println(jsonResponse);
        }
    }
    
    @SuppressWarnings("serial")
    /**
     * A servlet class handling GET requests with an error message and POST requests by calling the BFSTraversal class.
     * @author Rutger van Willigen
     * @author Bert Spaan
     */
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
        	response.setContentType("application/json");
        	response.setStatus(HttpServletResponse.SC_OK);
        	
        	// Read raw input
        	String line = null;
        	try {
        		BufferedReader reader = request.getReader();
        		while ((line = reader.readLine()) != null) jb.append(line);
        	} catch (Exception e) {	  
        		out.println(errorResponse("Error while parsing POST request: " + e.getMessage()));
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
            	out.println(errorResponse("Invalid JSON received."));
            } catch (IOException e) {
            	out.println(errorResponse(e.getMessage()));
            }
        }
        
        public String errorResponse (String errorMsg) {
        	JSONObject out = new JSONObject();
        	out.put("message", errorMsg);
        	out.put("status", "error");
        	return out.toString();
        }
    }
}