package cis5550.webserver;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.io.*;
import cis5550.tools.*;


class Worker implements Runnable {
	private static final Logger logger = Logger.getLogger(Server.class);
	private Socket sock;
	private int port;
	private String dir;


	public Worker(Socket sock, String dir) {
		this.sock = sock;
		this.dir = dir;
	}
	public void respondWithError(Socket s, String statusCode, String desc) throws Exception {
		PrintWriter pout = new PrintWriter(s.getOutputStream());
		pout.print(String.format("HTTP/1.1 %s %s\r\n", statusCode, desc));
		pout.print("Content-Length: 0\r\n\r\n");
		pout.flush();
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
				if (reqInfo[0].equals("POST") || reqInfo[0].equals("PUT")) {
					logger.error("Error 405 (Method Not Allowed): " + reqInfo[0]);
					try {
						respondWithError(sock, "405", "Error 405 (Method Not Allowed)");
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
				}
				if (headers.containsKey("content-length")) {				
					int cl = Integer.parseInt(headers.get("content-length"));
					for (int i = 0; i < cl; i++) {
						b = in.read();
					}
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
					int byteRead;
					logger.info("Begin to send");
					OutputStream out = sock.getOutputStream();
					PrintWriter pout = new PrintWriter(out);
					pout.print("HTTP/1.1 200 OK\r\n");
					pout.print("Content-Type: " + contentType + "\r\n");
					pout.print("Server: XXX\r\n");
					pout.print("Content-Length: "+ length + "\r\n\r\n");
					pout.flush();
					if (reqInfo[0].equals("HEAD")) {
						continue;
					}
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
				}

			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} 
		}
	}
}

public class Server {
	private static final Logger logger = Logger.getLogger(Server.class);

	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.println("Written by Yuting Zheng");
			System.exit(1);
		}
		logger.info("the port number is " + args[0] + ", and the name of directory is " + args[1]);
		int port = Integer.parseInt(args[0]);
		
		ServerSocket ssock = new ServerSocket(port);
		Socket sock = null;
//		sock.close();
		BlockingQueue<Socket> queue = new ArrayBlockingQueue<>(10);
		while (true) {
			sock = ssock.accept();
			queue.put(sock);
			while (!queue.isEmpty()) {				
				Worker worker = new Worker(queue.take(), args[1]);
				Thread thread = new Thread(worker);
				thread.start();
			}
//			sock.close();
		}
		
//		sock.close();
//		ssock.close();
		
	}
}