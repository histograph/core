package org.waag.histograph.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.waag.histograph.reasoner.ReasoningDefinitions;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;
import org.waag.histograph.util.BCrypt;

/**
 * This is a class initializing the Neo4j graph connection. This includes setting up indices if
 * they did not exist and the server for the Neo4j front end page in which Cypher queries can be executed.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
@SuppressWarnings("deprecation")
public class GraphInit {
	
	/**
	 * Static method that initializes the Neo4j graph based on a Configuration parameter and returns 
	 * the GraphDatabaseService associated object.
	 * @param config The Configuration object in which Neo4j file paths and ports should be specified.
	 * @return The GraphDatabaseService object of the initialized Neo4j graph.
	 * @throws Exception Thrown if the graph database could not be started at the given file path.
	 */
	public static GraphDatabaseService initNeo4j (Configuration config) throws Exception {
		try {
			GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(config.NEO4J_FILEPATH);
	        initializeIndices(db);
          createAdminOwner(config.ADMIN_NAME, config.ADMIN_PASSWORD, db);
	        initializeServer(config, db);
	        return db;
		} catch (RuntimeException e) {
			throw new Exception("Unable to start graph database on " + config.NEO4J_FILEPATH + ". " + e.getMessage());
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

  private static void createAdminOwner (String name, String password, GraphDatabaseService db) {
		ExecutionEngine engine = new ExecutionEngine(db);

		try ( Transaction tx = db.beginTx()) {
      Map<String, Object> params = new HashMap<String, Object>();
			params.put("name", name);
			params.put("password", BCrypt.hashpw(password, BCrypt.gensalt()));
      String query = "MERGE (admin:Owner { name:{name} }) ON CREATE SET admin.password = {password}";
			engine.execute(query, params);

      tx.success();
    }
  }

  private static void initializeIndices (GraphDatabaseService db) {
		try (Transaction tx = db.beginTx()) {
			Schema schema = db.schema();
			if (!schema.getConstraints(HistographTokens.Labels.PIT).iterator().hasNext()) {
				schema.constraintFor(HistographTokens.Labels.PIT).assertPropertyIsUnique(HistographTokens.General.HGID).create();
				schema.indexFor(HistographTokens.Labels.PIT).on(HistographTokens.PITTokens.URI).create();
			}

			if (!schema.getConstraints(HistographTokens.Labels.Owner).iterator().hasNext()) {
        schema.constraintFor(HistographTokens.Labels.Owner).assertPropertyIsUnique(HistographTokens.General.NAME).create();
			}

      if (!schema.getConstraints(HistographTokens.Labels.Source).iterator().hasNext()) {
				schema.constraintFor(HistographTokens.Labels.Source).assertPropertyIsUnique(HistographTokens.General.SOURCEID).create();
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
