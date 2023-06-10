package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.util.ArrayList;
import java.util.HashMap;

public class Master extends cis5550.generic.Master {
	static HashMap<String,String> activeNode = new HashMap<String,String>();
	
	public static void main(String args[]) {
		port(Integer.parseInt(args[0]));
		registerRoutes();
		get("/", (req, res) -> {
			return workerTable();
		});
	}
}
