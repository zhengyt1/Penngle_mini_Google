package cis5550.generic;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import static cis5550.webserver.Server.get;

public class Master {
//	static List<String> workerList = new ArrayList<String>();
	static ConcurrentHashMap<String,ConcurrentHashMap<String,String>> workerMap = new ConcurrentHashMap<String,ConcurrentHashMap<String,String>>();
	public static ArrayList<String> getWorkers() {
		workerMap.entrySet().removeIf(e -> Long.parseLong(e.getValue().get("accessTime")) + 15000 < System.currentTimeMillis());
		ArrayList<String> ret = new ArrayList<String>();
		for (String key: workerMap.keySet()) {
			ret.add(workerMap.get(key).get("ip") +":"+ workerMap.get(key).get("port"));
		}
		return ret;
	}
	public static String workerTable() {
		workerMap.entrySet().removeIf(e -> Long.parseLong(e.getValue().get("accessTime")) + 15000 < System.currentTimeMillis());
		ArrayList<String> ret = new ArrayList<String>();
		ret.add("<html>");
		ret.add("<table>");
		for (String key: workerMap.keySet()) {
			ret.add("<tr><td><div>" + key +"</div></td>");
			ret.add("<td><div>" + workerMap.get(key).get("ip") +"</div></td>");
			ret.add("<td><div>" + workerMap.get(key).get("port") +"</div></td>");
			ret.add("<td><a href=" + "http://" + workerMap.get(key).get("ip") +":"+ workerMap.get(key).get("port") + "/" + key +">" + key +"</a></td></tr>");
		}
		ret.add("</table>");
		ret.add("</html>");
		return String.join("", ret);
	}
	public static void registerRoutes() {
		get("/ping", (req, res) -> {
			System.out.println(req.ip()+"in /ping .........."+req.queryParams("port")+"!"+req.queryParams("id"));
			ConcurrentHashMap<String,String> entry = new ConcurrentHashMap<String,String>();
			
			if (req.ip() == null || req.queryParams("port") == null || req.queryParams("id") == null) {
				res.status(400, "id/port missing");
				return "400";
			}
			entry.put("ip", req.ip());
			entry.put("port", req.queryParams("port"));
			entry.put("accessTime", String.valueOf(System.currentTimeMillis()));
			workerMap.put(req.queryParams("id"), entry);
			return "OK"; 
		});
		get("/workers", (req, res) -> {
			workerMap.entrySet().removeIf(e -> Long.parseLong(e.getValue().get("accessTime")) + 15000 < System.currentTimeMillis());
			ArrayList<String> ret = new ArrayList<String>();
			ret.add(String.valueOf(workerMap.size()));
			for (String key: workerMap.keySet()) {
				ret.add(key + "," + workerMap.get(key).get("ip") +":"+ workerMap.get(key).get("port"));
			}
			return String.join("\n", ret)+"\n";
		});
		
	}
}
