package org.waag.histograph.pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.waag.histograph.util.Configuration;

public class PGInit {
	public static Connection initPG (Configuration config) throws SQLException {
		String url = "jdbc:postgresql://" + config.PG_HOST + "/" + config.PG_DB + "?user=" + config.PG_USER;
		if (!config.PG_PASS.equals("")) {
			url = url + "&password=" + config.PG_PASS;
		}
		
		return DriverManager.getConnection(url);
	}	
}