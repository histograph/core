package org.waag.histograph.graph;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.waag.histograph.queue.NDJSONTokens;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.util.Configuration;

@SuppressWarnings("deprecation")
public class GraphInit {
		
	public static GraphDatabaseService initNeo4j (Configuration config) {
		try {
			GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(config.NEO4J_FILEPATH);
	        initializeIndices(db);
	        initializeServer(config, db);
	        return db;
		} catch (RuntimeException e) {
			System.out.println("Unable to start graph database on " + config.NEO4J_FILEPATH + ".");
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	private static void initializeServer (Configuration config, GraphDatabaseService db) {
		WrappingNeoServerBootstrapper neoServerBootstrapper;
        
        try {
        	GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        	ServerConfigurator serverConfig = new ServerConfigurator(api);
            serverConfig.configuration().addProperty(Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, "localhost");
            serverConfig.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, config.NEO4J_PORT);
        
            neoServerBootstrapper = new WrappingNeoServerBootstrapper(api, serverConfig);
            neoServerBootstrapper.start();
        } catch (Exception e) {
        	System.out.println("Server exception: " + e.getMessage());
        }
        
        System.out.println("Neo4j listening on http://localhost:" + config.NEO4J_PORT + "/");
	}
	
	private static void initializeIndices (GraphDatabaseService db) {		
		try (Transaction tx = db.beginTx()) {
			Schema schema = db.schema();
			if (!schema.getConstraints(ReasoningDefinitions.NodeType.PIT).iterator().hasNext()) {
				schema.constraintFor(ReasoningDefinitions.NodeType.PIT).assertPropertyIsUnique(NDJSONTokens.General.HGID).create();
				schema.indexFor(ReasoningDefinitions.NodeType.PIT).on(NDJSONTokens.PITTokens.NAME).create();
			}
			tx.success();
		}
		
		try (Transaction tx = db.beginTx()) {
		    Schema schema = db.schema();
		    schema.awaitIndexesOnline(10, TimeUnit.MINUTES);
		    tx.success();
		}
	}
}
