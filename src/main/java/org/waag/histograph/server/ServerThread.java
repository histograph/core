package org.waag.histograph.server;

import java.sql.Connection;

import org.neo4j.graphdb.GraphDatabaseService;
import org.waag.histograph.util.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * A single thread class that acts as a server for the Traversal API.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class ServerThread implements Runnable {
	
	private static final String NAME = "ServerThread";

	static final String TRAVERSAL_PATH = "/traversal";
	static final String REJECTED_EDGES_PATH = "/rejected";
	
	GraphDatabaseService db;
	Connection pg;
	Configuration config;
	String version;
	boolean verbose;
	
	/**
	 * Constructor of the thread.
	 * @param db The Neo4j graph object on which the operations should be performed.
	 * @param pg The PostgreSQL connection object to which PG queries are sent.
	 * @param config The Configuration object in which the server's port number is specified.
	 * @param version The version of Histograph-Core.
	 * @param verbose Boolean value expressing whether the Server thread should write verbose output to stdout.
	 */
	public ServerThread(GraphDatabaseService db, Connection pg, Configuration config, String version, boolean verbose) {
		this.db = db;
		this.pg = pg;
		this.config = config;
		this.version = version;
		this.verbose = verbose;
	}

	/**
	 * The thread's main method. Starts listening on the port defined in the Configuration object provided in the 
	 * Thread's constructor and accepts traversal requests through POST requests at the traversal path.
	 */
    public void run() {
        Server server = new Server(config.TRAVERSAL_PORT);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        
        context.addServlet(new ServletHolder(new BaseServlet()), "/");
        context.addServlet(new ServletHolder(new TraversalServlet(db)), TRAVERSAL_PATH);
        context.addServlet(new ServletHolder(new RejectedEdgesServlet(pg)), REJECTED_EDGES_PATH);

        server.setHandler(context);
    	
        try {
			server.start();
	        System.out.println("Traversal API server listening on http://localhost:" + config.TRAVERSAL_PORT + "/");
	        server.join();
		} catch (Exception e) {
			namePrint("Error: " + e.getMessage());
			if (verbose) e.printStackTrace();
			System.exit(1);
		}
    }
    
    private void namePrint(String message) {
		System.out.println("[" + NAME + "] " + message);
	}
}