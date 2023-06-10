package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker{
	
	public static void main(String args[]) {
		if (args.length < 3) {
			System.exit(0);
		}
		port(Integer.parseInt(args[0]));
		String dir = args[1];
		ConcurrentHashMap<String, ConcurrentHashMap<String, Row>>tables = new ConcurrentHashMap<String, ConcurrentHashMap<String, Row>>();
		ConcurrentHashMap<String, String> persist_tables = new ConcurrentHashMap<String, String>();
		File worker_file = new File(dir);
		FileFilter filter = new FileFilter() {
            public boolean accept(File f)
            {
                return f.getName().endsWith(".table");
            }
        };
		for (File file: worker_file.listFiles(filter)) {	
			try {
				String table_name = file.getName().replace(".table","");
				tables.put(table_name, new ConcurrentHashMap<String, Row>());
				persist_tables.put(table_name, dir+"/"+table_name+".table");
				FileInputStream in = new FileInputStream(file);
				try {
					while (true) {
						long pos = in.getChannel().position();
						Row new_row = Row.readFrom(in);
						if (new_row == null) {
							break;
						}
						Row row = new Row(new_row.key());
						row.put("pos", String.valueOf(pos));
						tables.get(table_name).put(new_row.key(), row);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		File id_file = new File(dir+"/id");
		if (!id_file.exists()) {
			try (FileOutputStream fileOutputStream = new FileOutputStream(id_file)) {
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
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			StringBuffer sb = new StringBuffer();
			try (FileInputStream fileInputStream = new FileInputStream(id_file)) {
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
				String path = dir + File.separator + T + ".table";
				RandomAccessFile log = new RandomAccessFile(path, "rw");

				Row row = new Row(R);
				if (persist_tables.containsKey(T)) {
					if (row.get("pos") != null) {
						long old_pos = Long.parseLong(tables.get(T).get(R).get("pos"));
						log.seek(old_pos);
						row = Row.readFrom(log);
					}
					tables.get(T).get(R).put("pos", String.valueOf(log.length()));
				} else {
					row = tables.get(T).get(R);
					tables.get(T).get(R).put(C, req.bodyAsBytes());
				}
				row.put(C, req.bodyAsBytes());
				log.seek(log.length());
				log.write(row.toByteArray());
				log.seek(log.length());
				log.write(0x0A);
				return "OK";
			});
			get("/data/:T/:R/:C", (req, res) -> {
				String T = req.params("T");
				String R = req.params("R");
				String C = req.params("C");
				if (persist_tables.containsKey(T)) {
					if (tables.containsKey(T) && tables.get(T).containsKey(R)) {
						long pos = Long.parseLong(tables.get(T).get(R).get("pos"));
						RandomAccessFile persist_table = new RandomAccessFile(persist_tables.get(T), "r");
						persist_table.seek(pos);
						String[] row = persist_table.readLine().split(" ");
						res.body(row[row.length-1]);
						return null;
					}
					res.status(404, "no data in T/R/C");
					return "404";
				} else {
					if (tables.containsKey(T) && tables.get(T).containsKey(R) && tables.get(T).get(R).get(C) != null) {
						res.bodyAsBytes(tables.get(T).get(R).getBytes(C));
						return null;
					} else {
						res.status(404, "no data in T/R/C");
						return "404";
					}
				}
			});
			get("/data/:T/:R", (req, res) -> {
				String T = req.params("T");
				String R = req.params("R");
				if (persist_tables.containsKey(T)) {
					if (tables.containsKey(T) && tables.get(T).containsKey(R)) {
						long pos = Long.parseLong(tables.get(T).get(R).get("pos"));
						RandomAccessFile persist_table = new RandomAccessFile(persist_tables.get(T), "r");
						persist_table.seek(pos);
						res.body(persist_table.readLine());
						return null;
					}
					res.status(404, "no data in T/R");
					return "404";
				} else {
					if (tables.containsKey(T) && tables.get(T).containsKey(R)) {
						res.bodyAsBytes(tables.get(T).get(R).toByteArray());
						return null;
					} 
					res.status(404, "no data in T/R");
					return "404";
				}
			});
			get("/data/:T", (req, res) -> {
				String T = req.params("T");
				String startRow = req.queryParams("startRow");
				String endRowExclusive = req.queryParams("endRowExclusive");
				if (persist_tables.containsKey(T)) {
					if (tables.containsKey(T)) {
						for (String R: tables.get(T).keySet()) {
							long pos = Long.parseLong(tables.get(T).get(R).get("pos"));
							RandomAccessFile persist_table = new RandomAccessFile(persist_tables.get(T), "r");
							persist_table.seek(pos);
							Row row = Row.readFrom(persist_table);
							if ((startRow == null || row.key().compareTo(startRow) >= 0) && (endRowExclusive == null || row.key().compareTo(endRowExclusive) < 0)) {
								res.write(row.toByteArray());
								res.write("\n".getBytes());
							}
						}
						res.write("\n".getBytes());
						return null;
					} else {
						return "404";
					}
				} else {
					if (tables.containsKey(T)) {
						for (Row row: tables.get(T).values()) {
							if ((startRow == null || row.key().compareTo(startRow) >= 0) && (endRowExclusive == null || row.key().compareTo(endRowExclusive) < 0)) {
								res.write(row.toByteArray());
								res.write("\n".getBytes());
							}
						}
						res.write("\n".getBytes());
						return null;
					}
					res.status(404, "no data in T");
					return "404";
				}
			});
			put("/data/:T", (req, res) -> {
				String T = req.params("T");
				String path = dir + File.separator + T + ".table";
				if (!tables.containsKey(T)) {
					tables.put(T, new ConcurrentHashMap<String, Row>());
				} 
				RandomAccessFile log = new RandomAccessFile(path, "rw");
				// long pos = persist_table.length();
				String[] rows = req.body().split("\n");
				for (int i = 0; i < rows.length; i++) {
					String row_name = rows[i].split(" ")[0];
					if (!tables.get(T).containsKey(row_name)) {
						tables.get(T).put(row_name, new Row(row_name));
					}
					InputStream targetStream = new ByteArrayInputStream(rows[i].getBytes());
					Row row = Row.readFrom(targetStream);
					if (persist_tables.containsKey(T)) {
						if (row.get("pos") != null) {
							log.seek(Long.parseLong(row.get("pos")));
							Row old_row = Row.readFrom(log);
							for (String col: old_row.columns()) {
								if (row.get(col) == null) {
									row.put(col, old_row.get(col));
								}
							}
						}
						tables.get(T).get(row_name).put("pos", String.valueOf(log.length()));
					} else {
						for (String col: tables.get(T).get(row_name).columns()) {
							if (row.get(col) == null) {
								row.put(col, tables.get(T).get(row_name).get(col));
							}
						}
						tables.get(T).put(row_name, row);
					}
					log.seek(log.length());
					log.write(row.toByteArray());
					log.seek(log.length());
					log.write(0x0A);
				}
				return "OK";
			});
			get("/", (req, res) -> {
				ArrayList<String> ret = new ArrayList<String>();
				// ArrayList<String> table_names = new ArrayList<String>(tables.get(table).keySet());
				// Collections.sort(table_names);
				// for (String row: table_names) {
				// 	if (fromRow != null && row.compareTo(fromRow) == -1) {
				// 		continue;
				// 	}
				// 	for (String col: tables.get(table).get(row).columns()) {
				// 		columns.put(col, col);
				// 	}
				// }
				ret.add("<html><table>");
				for (String key: tables.keySet()) {
					String is_persist = "";
					if (persist_tables.containsKey(key)) {
						is_persist = "persist";
					}
					ret.add("<tr><td><a href=/view/" + key + ">" + key + "</a></td>"+ "<td>" +tables.get(key).size() + "</td><td>" + is_persist+"</td></tr>");
				}
				ret.add("</html></table>");
				return String.join("", ret);
			});
			get("/view/:T", (req, res) -> {
				String T = req.params("T");
				String path = dir + File.separator + T + ".table";
				String fromRow = req.queryParams("fromRow");
				if (!tables.containsKey(T)) {
					res.status(404, "not found");
					return "404";
				}
				// System.out.println(fromRow);
				ArrayList<Row> rows =  new ArrayList<Row>();
				if (persist_tables.containsKey(T)) {
					try (RandomAccessFile log = new RandomAccessFile(path, "rw")) {
						for (Row row: tables.get(T).values()) {
							if (fromRow == null || row.key().compareTo(fromRow) >= 0) {
								log.seek(Long.parseLong(row.get("pos")));
								rows.add(Row.readFrom(log));
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					for (Row new_row: tables.get(T).values()) {
						if (fromRow == null || new_row.key().compareTo(fromRow) >= 0) {
							rows.add(new_row);
						}
					}
				}
				rows.sort((r1, r2) -> r1.key().compareTo(r2.key()));
				// System.out.println(rows.size());
				// System.out.println(rows);
				// int page = 0;
				// if (rows.size() > 10) {
				// 	if (req.queryParams("page") != null) {
				// 		page = Integer.parseInt(req.queryParams("page"));
				// 	} else {
				// 		res.redirect("/view/"+T+"?page=0", 302);
				// 		// res.status(200, "OK");
				// 		// return "OK";
				// 	}
				// }
				ArrayList<String> ret = new ArrayList<String>();
				ret.add("<html><h1>" + T + "</h1><table>");
				HashSet<String> columns_set = new HashSet<String>();
				for (int i = 0; i < Math.min(10, rows.size()); i++) {
					Row row = rows.get(i);
					// if (fromRow != null && row.compareTo(fromRow) == -1) {
					// 	continue;
					// }
					for (String col: row.columns()) {
						columns_set.add(col);
					}
				}
				ArrayList<String> columns = new ArrayList<String>(columns_set);
				columns.sort((c1, c2) -> c1.compareTo(c2));
				ret.add("<tr><td>rows</td>");
				for (String col: columns) {
					ret.add("<td>" + col + "</td>");
				}
				ret.add("</tr>");
				for (int i = 0; i < Math.min(10, rows.size()); i++) {
					Row row = rows.get(i);
					ret.add("<tr><td>" + row.key() + "</td>");
					for (String col: columns) {
						ret.add("<td>" + row.get(col) + "</td>");
					}
					ret.add("</tr>");
				}
				ret.add("</table>");
				if (10 < rows.size()) {
					ret.add("<a href=/view/"+T+"?fromRow=" + rows.get(10).key() +">Next</a>");
				}
				ret.add("</html>");
				return String.join("", ret);
			});
			put("/persist/:table", (req, res) -> {
				String table = req.params("table");
				if (tables.containsKey(table)) {
					res.status(403, "Already exists");
					return "403 already exist";
				}
				tables.put(table, new ConcurrentHashMap<String, Row>());
				persist_tables.put(table, dir+"/"+table+".table");
				File new_table = new File(dir+"/"+table+".table");
				if (!new_table.exists()) {
					new_table.createNewFile();
				}
				res.status(200, "OK");
				return "OK";
			});
			put("/rename/:T", (req, res) -> {
				String T = req.params("T");
				String new_name =  req.body();
				String path = dir + File.separator + T + ".table";
				if (tables.containsKey(T)) {
					if (tables.containsKey(new_name)) {
						return "409";
					}
					tables.put(new_name, tables.get(T));
					tables.remove(T);
					// if (persist_tables.containsKey(T)) 
					int pos = path.lastIndexOf(T);
					File table = new File(path);
					if (!table.exists()) {
						return "404";
					}
					File new_table = new File(path.substring(0, pos) + new_name + ".table");
					if (new_table.exists()) {
						return "409";
					}
					table.renameTo(new_table);
					return "OK";
				} else {
					return "404";
				}
			});
			get("/count/:T", (req, res) -> {
				String T = req.params("T");
				if (tables.containsKey(T)) {
					return String.valueOf(tables.get(T).size());
				}
				return "404";
			});
			put("/delete/:T", (req, res) -> {
				String T =  req.params("T");
				String path = dir + File.separator + T + ".table";
				if (tables.containsKey(T)) {
					tables.remove(T);
					File to_be_deleted = new File(path); 
					if (to_be_deleted.delete()) { 
						System.out.println("Deleted the file: " + to_be_deleted.getName());
					} else {
						System.out.println("Failed to delete the file.");
						res.status(404, "File not found");
						return "404";
					} 
					res.status(200, "OK");
					return "OK";
				} else {
					res.status(404, "File not found");
					return "404";
				}
			});
			get("/tables", (req, res) -> {
				res.type("text/plain");
				return String.join("\n", tables.keySet()) + "\n";
			});
			startPingThread(args[2], id, args[0]);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
