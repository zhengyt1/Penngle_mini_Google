package cis5550.webserver;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import cis5550.tools.Logger;

class Worker implements Runnable {
	private static final Logger logger = Logger.getLogger(Server.class);
	private Socket sock;
	private String dir;
	private String host;
	private HashMap<String, Route> routeMap; 
	private String splitSymbol;

	public Worker(Socket sock, String dir, HashMap<String, Route> routeMap, String splitSymbol, String host) {
		this.sock = sock;
		this.dir = dir;
		this.routeMap = routeMap;
		this.splitSymbol =  splitSymbol;
	}
	public void respondWithError(Socket s, String statusCode, String desc) throws Exception {
		PrintWriter pout = new PrintWriter(s.getOutputStream());
		pout.print(String.format("HTTP/1.1 %s %s\r\n", statusCode, desc));
		pout.print("Content-Length: 0\r\n\r\n");
		pout.flush();
	}
	public void staticFileRes(String[] reqInfo) {
		if (reqInfo[0].equals("POST") || reqInfo[0].equals("PUT")) {
			logger.error("Error 405 (Method Not Allowed): " + reqInfo[0]);
			try {
				respondWithError(sock, "405", "Error 405 (Method Not Allowed)");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		String path = dir + reqInfo[1];
		File file = new File(path);
		long length = file.length();
		boolean exists = file.exists();
		if (!exists) {
			logger.error("Error 404, file not found");
			try {
				respondWithError(sock, "404", "Error 404 (Not Found)");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		String[] urlSplit = reqInfo[1].split("\\.");
		String extension = urlSplit[urlSplit.length - 1];
		String contentType = "application/octet-stream";
		if (extension.equals("jpg") || extension.equals("jpeg")) {
			contentType = "image/jpeg";
		} else if (extension.equals("txt")) {
			contentType = "text/plain";
		} else if (extension.equals("html")) {
			contentType = "text/html";
		} 
		try {
			byte[] fileBuffer = new byte[(int) length];
			FileInputStream fileInputStream = new FileInputStream(file);
			logger.info("Begin to send");
			OutputStream out = sock.getOutputStream();
			PrintWriter pout = new PrintWriter(out);
			pout.print("HTTP/1.1 200 OK\r\n");
			pout.print("Content-Type: " + contentType + "\r\n");
			pout.print("Server: XXX\r\n");
			pout.print("Content-Length: "+ length + "\r\n\r\n");
			pout.flush();
			if (reqInfo[0].equals("HEAD")) {
				return;
			}
			int byteRead;
			while ((byteRead = fileInputStream.read(fileBuffer)) != -1) {
				out.write(fileBuffer, 0, byteRead);
			}
			fileInputStream.close();
		} catch (FileNotFoundException e){
			logger.error(e.toString());
			try {
				logger.error("Error 403 (Permission denied)");
				respondWithError(sock, "403", "Error 403 (Permission denied)");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		int flag = 0;
		while (flag == 0) {
			logger.info("Incoming connection from " + sock.getRemoteSocketAddress());
			logger.info("Ready to receive data");
			InputStream in = null;
			try {
				in = sock.getInputStream();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			int matchPtr = 0;
			int b = 0;
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			while (matchPtr < 4 && b != -1) {			
				try {
					b = in.read();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (b < 0) {
					logger.info("b < 0, close socket and open another");
					try {
						sock.close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//					sock = ssock.accept();
					flag = 1;
					break;
				}
				buffer.write(b);
				if ((((matchPtr==0) || (matchPtr==2)) && (b=='\r')) || (((matchPtr==1) || (matchPtr==3)) && (b=='\n')))
					matchPtr ++;
				else
					matchPtr = 0;
			}
			if (flag == 1) {
//				flag = 0;
				continue;
			}
			logger.info("Incoming: " + buffer);
			try {
				BufferedReader hdr = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.toByteArray())));
				buffer.reset();
				String firstLine;
				firstLine = hdr.readLine(); // GET /file1.txt HTTP/1.1
			
				String[] reqInfo = firstLine.split(" ");
				if (reqInfo.length < 3 || !reqInfo[2].startsWith("HTTP")) {
					logger.error("Error 400 (Bad Request), method, URL, protocol missing");
					try {
						respondWithError(sock, "400", "Error 400 (Bad Request)");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				Set<String> methodSet = new HashSet<String>();
				methodSet.add("GET");
				methodSet.add("HEAD");
				methodSet.add("PUT");
				methodSet.add("POST");
				if (!methodSet.contains(reqInfo[0])) {
					logger.error("Error 501 (Not Implemented): " + reqInfo[0]);
					try {
						respondWithError(sock, "501", "Error 501 (Not Implemented)");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				if (!reqInfo[2].equals("HTTP/1.1")) {
					logger.error("Error 505 (Version Not Supported)");
					try {
						respondWithError(sock, "505", "Error 505 (Version Not Supported)");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				if (reqInfo[1].contains("..")) {
					logger.error("Error 403, (Permission denied)");
					try {
						respondWithError(sock, "403", "Error 403 (Permission denied)");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				HashMap<String, String> headers = new HashMap<String, String>();
				while (true) {
					String line = hdr.readLine();
					if (line.equals("")) {
						break;
					}
					String[] kv = line.split(":");
					if (kv.length == 2) {
						headers.put(kv[0].toLowerCase().trim(), kv[1].trim());
					} else {
						logger.error("Error 520 (Unknow Error)");
						try {
							respondWithError(sock, "520", "Error 520 (Unknow Error)");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				if (!headers.containsKey("host")) {
					logger.error("Error 400 Bad Request, no Host header");
					try {
						respondWithError(sock, "400", "Error 400 Bad Request, no Host header");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
				if (headers.containsKey("content-length")) {				
					int cl = Integer.parseInt(headers.get("content-length"));
					for (int i = 0; i < cl; i++) {
						b = in.read();
						buffer.write(b);
					}
				}
				byte[] reqBody =  buffer.toByteArray();
				String[] reqList = reqInfo[1].split("/");
				HashMap<String, String> queryParams = new HashMap<String, String>();
				HashMap<String, String> reqParams = new HashMap<String, String>();
				boolean isMatch = false;
				String routePath = "";
				for (String s: routeMap.keySet()) {
					isMatch = true;
					reqParams.clear();
					queryParams.clear();
					String[] sSplit = s.split(splitSymbol);
					if (sSplit.length != 2) {
						logger.error("Error 520: " + s);
						try {
							respondWithError(sock, "520", "Error 520 (Unknow Error)");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					String method = sSplit[0];
					routePath = sSplit[1];
					if (!method.equals(reqInfo[0])) {
						isMatch = false;
						continue;
					};
					String[] routeList = routePath.split("/");
					if (reqList.length != routeList.length) {
						isMatch = false;
						continue;
					}
					for (int i = 0; i < reqList.length; i++) {
						if (routeList[i].startsWith(":")) {
							reqParams.put(routeList[i].substring(1), reqList[i]);
						} else {
							String[] queryCheck = reqList[i].split("\\?");
							if (!routeList[i].equals(queryCheck[0])) {
								isMatch = false;
								break;
							}
							if (queryCheck.length == 2) {								
								String[] queryList = queryCheck[1].split("&");
								for (String query: queryList) {
									queryParams.put(URLDecoder.decode(query.split("=")[0], "UTF-8"), URLDecoder.decode(query.split("=")[1], "UTF-8"));
								}
							}
						}
					}
					if (isMatch) {
						break;
					}
				}
				if (headers.get("content-type") != null && headers.get("content-type").equals("application/x-www-form-urlencoded")) {
					String[] queryList = new String(reqBody).split("&");
					for (String query: queryList) {
						queryParams.put(URLDecoder.decode(query.split("=")[0], "UTF-8"), URLDecoder.decode(query.split("=")[1], "UTF-8"));
					}
				}
				if (isMatch) {
					Route r = routeMap.get(reqInfo[0] + splitSymbol + routePath);					
					if (r == null) System.out.print("error happens");
					RequestImpl req = new RequestImpl(reqInfo[0], reqInfo[1], reqInfo[2], headers, queryParams, reqParams, (InetSocketAddress) sock.getRemoteSocketAddress(), reqBody, null);
					ResponseImpl res = new ResponseImpl(sock);
					try {
						Object obj = r.handle(req, res);
						if (res.writeCalled) {
							sock.close();
							return;
						}
//						System.out.println(obj.toString());
//						ObjectOutputStream objOutputStream = new ObjectOutputStream((OutputStream) obj);
						OutputStream out = sock.getOutputStream();
						PrintWriter pout = new PrintWriter(out);
						pout.print("HTTP/1.1 " + res.statusCode + " " + res.reasonPhrase + "\r\n");
//						pout.print("Content-Type: " + "application/octet-stream" + "\r\n");
//						pout.print("Server: XXX\r\n");
						if (obj != null) {							
							pout.print("content-length: "+ obj.toString().length() + "\r\n");
						} else {
							if (res.body == null) {
								pout.print("content-length: 0\r\n");
							} else {								
								pout.print("content-length: "+ res.body.length + "\r\n");
							}
						}
						for (String i: res.headers.keySet()) {
							pout.print(String.format("%s: %s\r\n", i, res.headers.get(i)));
						}
						pout.print("\r\n");
						pout.flush();
						if (reqInfo[0].equals("HEAD")) {
							return;
						}
						if (obj != null) {							
							pout.print(obj.toString());
							pout.flush();
						} else if (res.body != null) {
							out.write(res.body, 0, res.body.length);
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						try {
							if (res.writeCalled) {
								sock.close();
								return;
							}
							respondWithError(sock, "500", " Internal Server Error");
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						e.printStackTrace();
					}
				} else {
//					System.out.println("static ....");
					staticFileRes(reqInfo);
				}


			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		}
	}
}

public class Server implements Runnable {
	private static final Logger logger = Logger.getLogger(Server.class);
	static Server server = null;
	static boolean flag = false;
	static HashMap<String, Route> routeMap = new HashMap<String, Route>();
	static String folder;
	static String splitSymbol = "_@#_";
	static int port = 80;
	static String host;
	
	public static void checkFlag() {
		if (flag == false) {
			routeMap.clear();
//			routeMap.put("GET", new HashSet<String>());
//			routeMap.put("PUT", new HashSet<String>());
//			routeMap.put("POST", new HashSet<String>());
			flag = true;
			Thread t = new Thread(server);
			t.start();
		}
	}
	public static class staticFiles {
		public static void location(String s) {
			folder = s;
			if (server == null) {
				server = new Server();
			}
			checkFlag();
		}
	}
	public static void host(String H) {
		host = H;
		if (server == null) {
			server = new Server();
		}
		checkFlag();
	}
	public static void get(String s, Route route) {
		if (server == null) {
			server = new Server();
		}
		checkFlag();
		routeMap.put("GET" + splitSymbol + s, route);
	}
	public static void post(String s, Route route) {
		if (server == null) {
			server = new Server();
		}
		checkFlag();
		routeMap.put("POST" + splitSymbol + s, route);
	}
	public static void put(String s, Route route) {
		if (server == null) {
			server = new Server();
		}
		checkFlag();
		routeMap.put("PUT" + splitSymbol + s, route);
	}
	public static void port(int p) {
		port = p;
		if (server == null) {
			server = new Server();
		}
	}
	public void run() {
		// TODO Auto-generated method stub
//		if (args.length != 2) {
//			System.out.println("Written by Yuting Zheng");
//			System.exit(1);
//		}
//		int port = port();
//		String folder = location("test");
		logger.info("the port number is " + port + ", and the name of directory is " + folder);
		
		ServerSocket ssock = null;
		try {
			ssock = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		Socket sock = null;
//		sock.close();
		BlockingQueue<Socket> queue = new ArrayBlockingQueue<>(10);
		while (true) {
			try {
				sock = ssock.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				queue.put(sock);
				while (!queue.isEmpty()) {				
					Worker worker = new Worker(queue.take(), folder, routeMap, splitSymbol, host);
					Thread thread = new Thread(worker);
					thread.start();
				}
			} 
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			sock.close();
		}
		
//		sock.close();
//		ssock.close();
		
	}
}