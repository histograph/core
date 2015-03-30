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

	static final String TRAVERSAL_PATH = "/traversal";
	static final String REJECTED_EDGES_PATH = "/rejected";
	
	static GraphDatabaseService db;
	static Connection pg;
	static Configuration config;
	static String version;
	
	/**
	 * Constructor of the thread.
	 * @param db The Neo4j graph object on which the operations should be performed.
	 * @param pg The PostgreSQL connection object to which PG queries are sent.
	 * @param config The Configuration object in which the server's port number is specified.
	 * @param version The version of Histograph-Core.
	 */
	public ServerThread(GraphDatabaseService db, Connection pg, Configuration config, String version) {
		ServerThread.db = db;
		ServerThread.pg = pg;
		ServerThread.config = config;
		ServerThread.version = version;
	}

	/**
	 * The thread's main method. Starts listening on the port defined in the Configuration object provided in the 
	 * Thread's constructor and accepts traversal requests through POST requests at the traversal path.
	 */
    public void run() {
        Server server = new Server(config.TRAVERSAL_PORT);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        
        context.addServlet(new ServletHolder(new BaseServlet()), "/");
        context.addServlet(new ServletHolder(new TraversalServlet(db)), TRAVERSAL_PATH);
        context.addServlet(new ServletHolder(new RejectedEdgesServlet(pg)), REJECTED_EDGES_PATH);
    	
        try {
			server.start();
	        System.out.println("Traversal API server listening on http://localhost:" + config.TRAVERSAL_PORT + "/");
	        server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}