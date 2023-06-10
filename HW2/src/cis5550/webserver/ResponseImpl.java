package cis5550.webserver;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;

import cis5550.tools.Logger;

public class ResponseImpl implements Response {
	int statusCode = 200;
	byte body[];
	HashMap<String, String> headers = new HashMap<String, String>();
	String reasonPhrase = "OK";
	Server server;
	boolean writeCalled = false;
	Socket sock;
	private static final Logger logger = Logger.getLogger(Server.class);
	 
	ResponseImpl(Socket sock) {
//		headers.put("content-type", reasonPhrase);
		this.sock = sock;
	}
    public void body(String body) {
    	this.body = body.getBytes();
    }
    public void bodyAsBytes(byte bodyArg[]) {
    	this.body = bodyArg;
    }
  
    public void header(String name, String value) {
    	headers.put(name.toLowerCase(), value);
    }
    public void type(String contentType) {
    	headers.put("content-type", contentType);
    }
  
    public void status(int statusCode, String reasonPhrase) {
    	this.statusCode = statusCode;
    	this.reasonPhrase = reasonPhrase;
    }
    
    public void write(byte[] b) throws Exception {
    	logger.info("in write");
    	OutputStream out = sock.getOutputStream();
    	if (writeCalled ==  false) {
			PrintWriter pout = new PrintWriter(out);
			pout.print("HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n");
    		for (String i: headers.keySet()) {
				pout.print(String.format("%s: %s\r\n", i, headers.get(i)));
			}
    		pout.print("connection: close\r\n");
    		pout.print("\r\n");
    		pout.flush();
    	}
    	out.write(b, 0, b.length);
    	writeCalled = true;
    }
    public void redirect(String url, int responseCode) {
    	this.statusCode = responseCode;
		headers.put("location", url);
    }
    public void halt(int statusCode, String reasonPhrase) {
    	
    }
}
