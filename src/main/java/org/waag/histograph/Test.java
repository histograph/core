package org.waag.histograph;

import java.io.IOException;

import org.apache.http.client.fluent.Request;

public class Test {
	
	public static void main(String[] argv) throws IOException {
		String url = "http://api.histograph.io/search?name=bussum";
		String response = Request.Get(url).execute().returnContent().asString();
		System.out.println(response);
	}

}
