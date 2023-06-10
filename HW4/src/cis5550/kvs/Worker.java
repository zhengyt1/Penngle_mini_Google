package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker{
	
	public static void main(String args[]) {
		if (args.length < 3) {
			System.exit(0);
		}
		port(Integer.parseInt(args[0]));
		String dir = args[1];
		File file = new File(dir+"/id");
		if (!file.exists()) {
//			System.out.println("creating ...");
			try {
				FileOutputStream fileOutputStream = new FileOutputStream(file);
				for (int i=0; i<5; i++) {
					try {
						fileOutputStream.write((int) ('a' + Math.random() * ('z' - 'a' + 1)));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			StringBuffer sb = new StringBuffer();
			try (FileInputStream fileInputStream = new FileInputStream(file)) {
				int b = 0;
				while (true) {
					try {
						b = fileInputStream.read();
						if (b < 0) {							
							break;
						}
						sb.append((char) b);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (FileNotFoundException e) {
				throw e;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String id = sb.toString();
//			System.out.println("id ... " + id);
			ConcurrentHashMap<String, ConcurrentHashMap<String, Row>>tables = new ConcurrentHashMap<String, ConcurrentHashMap<String, Row>>();
			put("/data/:T/:R/:C", (req, res) -> {
				String T = req.params("T");
				String R = req.params("R");
				String C = req.params("C");
				if (!tables.containsKey(T)) {
					tables.put(T, new ConcurrentHashMap<String, Row>());
				}
				if (!tables.get(T).containsKey(R)) {
					tables.get(T).put(R, new Row(R));
				}
//				System.out.println("table: "+req.body());
				tables.get(T).get(R).put(C, req.bodyAsBytes());
				return "OK";
			});
			get("/data/:T/:R/:C", (req, res) -> {
				String T = req.params("T");
				String R = req.params("R");
				String C = req.params("C");
				
				if (tables.containsKey(T) && tables.get(T).containsKey(R) && tables.get(T).get(R).get(C) != null) {
					res.bodyAsBytes(tables.get(T).get(R).getBytes(C));
					return null;
				} else {
					res.status(404, "no data in T/R/C");
					return "404";
				}
			});
			startPingThread(args[2], id, args[0]);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
