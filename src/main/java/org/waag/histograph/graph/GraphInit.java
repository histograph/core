package org.waag.histograph.graph;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.waag.histograph.util.BCrypt;
import org.waag.histograph.util.Configuration;
import org.waag.histograph.util.HistographTokens;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This is a class initializing the Neo4j graph connection. This includes setting up indices if
 * they did not exist and the server for the Neo4j front end page in which Cypher queries can be executed.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
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
			final GraphDatabaseService db = new GraphDatabaseFactory()
					.newEmbeddedDatabaseBuilder(config.NEO4J_FILEPATH)
					// these methods are not deprecated, but don't seem to do the trick
//					.setConfig(ServerSettings.webserver_address, "localhost")
//					.setConfig(ServerSettings.webserver_port, config.NEO4J_PORT)
					.newGraphDatabase();

			// cleanly shutdown Neo4J when this application is killed
			Runtime.getRuntime().addShutdownHook(new Thread(){
				@Override public void run(){
					db.shutdown();
				}
			});

			initializeIndices(db);
			createAdminOwner(config.ADMIN_NAME, config.ADMIN_PASSWORD, db);
			initializeServer(config, db);
	        return db;
		} catch (RuntimeException e) {
			throw new Exception("Unable to start graph database on " + config.NEO4J_FILEPATH + ". " + e.getMessage());
		}
	}

	@SuppressWarnings("deprecation")
	private static void initializeServer (Configuration config, GraphDatabaseService db) {
		try {
			GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        	ServerConfigurator serverConfig = new ServerConfigurator((GraphDatabaseAPI) db);
            serverConfig.configuration().addProperty(Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY, "localhost");
            serverConfig.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, config.NEO4J_PORT);

          final WrappingNeoServerBootstrapper server = new WrappingNeoServerBootstrapper(api, serverConfig);
			server.start();
        } catch (Exception e) {
        	System.out.println("Server exception: " + e.getMessage());
        }

        System.out.println("Neo4j listening on http://localhost:" + config.NEO4J_PORT + "/");
	}

	private static void createAdminOwner (String name, String password, GraphDatabaseService db)
	{
		try(Transaction tx = db.beginTx())
		{
			final Map<String, Object> params = new HashMap<>();
			params.put("name", name);
			params.put("password", BCrypt.hashpw(password, BCrypt.gensalt()));

			final String q = "MERGE (admin:Owner { name:{name} }) ON CREATE SET admin.password = {password}";
			db.execute(q, params);

			tx.success();
		}
	}

	private static void initializeIndices (GraphDatabaseService db) {
		try(Transaction tx = db.beginTx())
		{
			final Schema schema = db.schema();

			if (!schema.getConstraints(HistographTokens.Labels.PIT).iterator().hasNext())
			{
				schema.constraintFor(HistographTokens.Labels.PIT)
						.assertPropertyIsUnique(HistographTokens.General.HGID)
						.create();

				schema.indexFor(HistographTokens.Labels.PIT)
						.on(HistographTokens.PITTokens.URI)
						.create();
			}

			if (!schema.getConstraints(HistographTokens.Labels.Owner).iterator().hasNext())
			{
        		schema.constraintFor(HistographTokens.Labels.Owner)
						.assertPropertyIsUnique(HistographTokens.General.NAME)
						.create();
			}

      		if (!schema.getConstraints(HistographTokens.Labels.Source).iterator().hasNext()) {
				schema.constraintFor(HistographTokens.Labels.Source)
						.assertPropertyIsUnique(HistographTokens.General.SOURCEID)
						.create();
      		}

			tx.success();
		}

		try(Transaction tx = db.beginTx())
		{
		    db.schema().awaitIndexesOnline(10, TimeUnit.MINUTES);
		    tx.success();
		}
	}
}