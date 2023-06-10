package cis5550.generic;

import static cis5550.webserver.Server.get;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL; 

class WorkerThread implements Runnable {
	String ip_port;
	String rand;
	String port;
	WorkerThread(String ip_port, String rand, String port) {
		this.ip_port = ip_port;
		this.rand = rand;
		this.port = port;
	}
	@Override
	public void run() {
		URL url;		
		try {
			url = new URL("http://"+ip_port+"/ping?id="+rand+"&port="+port);
//			System.out.println("Given Url is : "+url);  
//			System.out.println(url.getContent());
			url.getContent();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
public class Worker {
	public static void startPingThread(String ip_port, String rand, String port) {
		while (true) {
			WorkerThread workerThread = new WorkerThread(ip_port, rand, port);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Thread t = new Thread(workerThread);
			t.start();
		}
	}
}
