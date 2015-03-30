package org.waag.histograph.server;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

/**
 * A servlet class handling GET requests at the root path.
 * @author Rutger van Willigen
 * @author Bert Spaan
 */
public class BaseServlet extends HttpServlet {
	private static final long serialVersionUID = -1650124209656224539L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.setStatus(HttpServletResponse.SC_OK);
        PrintWriter out = response.getWriter();
        
        JSONObject jsonResponse = new JSONObject();
        jsonResponse.put("name", "histograph");
        jsonResponse.put("version", getClass().getPackage().getImplementationVersion());
        jsonResponse.put("message", "Histograph Traversal API. Send POST requests with body '{hgids: [hgid1, hgid2, ...]}'.");
        
        out.println(jsonResponse);
    }
}