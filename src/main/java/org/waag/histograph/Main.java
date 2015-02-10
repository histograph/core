package org.waag.histograph;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jGraphTraversal;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.structure.Vertex;

public class Main {
	
	Neo4jGraph g;
	GremlinServer s;
	
	public static void main(String[] argv) {
		new Main().start();
	}

	private void start() {
		System.out.println("halootjes");
				
		try {
			Settings settings = Settings.read(getClass().getResourceAsStream("/gremlin-server-neo4j.yaml"));
			
			System.out.println(settings.graphs);
			
			s = new GremlinServer(settings);
			
			s.run();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
//		g = Neo4jGraph.open("/tmp/neo4j");
//		Neo4jGraphTraversal<Vertex, String> hond = g.V().label();		
//		while (hond.hasNext()) {
//			System.out.println(hond.next());			
//		}
//		try {
//			g.close();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
		
		
		// 3. Start Gremlin Server
		// 4. En blijf lekker draaien!
		// 5. Haal neo4j-dir en andere toestanden uit config-bestand!
		
		
		
		
	}
	
}
