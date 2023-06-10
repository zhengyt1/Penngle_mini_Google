package cis5550.jobs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.URLParser;


public class Crawler {
	public static String normalize(String s, String base) {
	
		if (s.indexOf("#") != -1) {			
			s = s.substring(0, s.indexOf("#"));
		}
		if (s.endsWith(".jpg") || s.endsWith(".jpeg") || s.endsWith(".gif") || s.endsWith(".png") || s.endsWith(".txt"))
		if (s.equals("")) {
			return base;
		}
		String[] parse1 = URLParser.parseURL(s);
		String[] parse2 = URLParser.parseURL(base);
		String protocal1 = parse1[0];
		String host1 = parse1[1];
		String port1 = parse1[2];
		String whole1 = parse1[3];
		String protocal2 = parse2[0];
		String host2 = parse2[1];
		String port2 = parse2[2];
		String whole2 = parse2[3];
		String ret = "";
		if (protocal1 != null) {
			ret += protocal1 + ":";
		} else if (protocal2 != null){
			ret += protocal2 + ":";
		} else {
			System.out.println("something wrong, base: "+base+", new_uri: "+s);
			return "";
		}
		if (host1 != null) {
			ret += "//" + host1;
		} else if (host2 != null){
			ret += "//" + host2;
		} else {
			System.out.println("something wrong, base: "+base+", new_uri: "+s);
			return "";
		}
		if (port1 != null) {
			ret += ":" + port1;
		} else if (port2 != null){
			ret += ":" + port2;
		} else {
			ret += ret.contains("https") ? ":443" : ":80";
		}
		if (whole1 == null) {
			return base;
		} else {
			if (whole1.startsWith("/")) {
				ret += whole1;
			} else {
				ret += whole2.substring(0, whole2.lastIndexOf("/")) + "/" + whole1;
				while (ret.contains("..")) {
					String regex = "(.*)/..(/.*)";
					Matcher matcher = Pattern.compile(regex).matcher(ret);
					if (matcher.find()) {
						String front = matcher.group(1);
						String back = matcher.group(2);
						ret = front.substring(0, front.lastIndexOf("/")) + back;
					} else {
						System.out.println("something wrong2, base: "+base+" new uri: "+s);
						break;
					}
				}
			}
		}
		return ret;
	}
	public static String find_url(String content) {
	    String regex = "<[aA]\\s(.+=.+)*href=\"([^\"]*)\"(.+=.+)*\\s*>";
	    //Creating a pattern object
	    Pattern pattern = Pattern.compile(regex);
	    LinkedList<String> list = new LinkedList<String>();
        //Matching the compiled pattern in the String
	    Matcher matcher = pattern.matcher(content);
	    if (matcher.find()) {
	    	String url = matcher.group(2);
	        return url;
	    }
	    return "";
	}
	
	public static void run(FlameContext ctx, String[] arr) {
		if (arr.length != 1) {
			 ctx.output("Error in Crawler");
		} else {
			ctx.output("OK?");
			String kvs_master = ctx.getKVS().getMaster();
			try {
				ctx.getKVS().persist("crawl");
			} catch (Exception e) {
				e.printStackTrace();
			}
			FlameRDD urlQueue = null;
			List<String> list = new LinkedList<String>();
			list.add(normalize(arr[0], ""));
			try {
				urlQueue = ctx.parallelize(list);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				int count = 0;
				while (urlQueue.count() != 0) {
					System.out.println(count+"-----------------------------------");
					count += 1;
		    		urlQueue = urlQueue.flatMap(u-> {
						KVSClient kvs = new KVSClient(kvs_master);
						LinkedList<String> new_url_list = new LinkedList<String>();
				    	if (kvs.existsRow("crawl", String.valueOf(u.hashCode()))) {
				    		return new_url_list;
				    	}
				    	String[] parsed_url = URLParser.parseURL(u);
				    	List<HashMap<String, String>> robot_list = new LinkedList<HashMap<String, String>>();
				    	if (kvs.existsRow("hosts", parsed_url[1])) {
				    		String robots_txt = new String(kvs.get("hosts", parsed_url[1], "robots_txt"));
				    		boolean flag = false;
				    		long time_interval = 1000;
				    		for (String line: robots_txt.split("\n")) {
				    			String[] split = line.replace(" ","").toLowerCase().split(":");
				    			if (split.length != 2) {
				    				continue;
				    			}
				    			if (split[0].equals("user-agent")) {				    				
				    				if (split[1].equals("cis5550-crawler") || split[1].equals("*")) {
				    					flag = true;
				    				} else {
				    					flag = false;
				    				}
				    			}
				    			else if (flag) {
				    				if (split[0].equals("allow")) {
				    					if (parsed_url[3].startsWith(split[1])) {
				    						break;
				    					}
				    				}
				    				if (split[0].equals("disallow")) {
				    					if (parsed_url[3].startsWith(split[1])) {
				    						return new_url_list;
				    					}
				    				}
				    				if (split[0].equals("crawl-delay")) {
				    					time_interval = Long.parseLong(split[1])*1000;
				    				}
				    			}
				    		}
				    		if (System.currentTimeMillis() - Long.parseLong(new String(kvs.get("hosts", parsed_url[1], "last_accessed_time"))) < time_interval) {
				    			new_url_list.add(u);
				    			return new_url_list;
				    		}
				    	} else {
				    		HttpURLConnection robot_con = (HttpURLConnection)(new URL(parsed_url[0]+"://"+parsed_url[1]+":"+parsed_url[2]+"/robots.txt")).openConnection();
				    		robot_con.setRequestMethod("GET");
				    		robot_con.setDoOutput(true);
//				    		robot_con.setRequestProperty("Content-Type", "application/jar-archive");
				    		robot_con.setRequestProperty("User-Agent", "cis5550-crawler");
				    		robot_con.setInstanceFollowRedirects(false);
				    		robot_con.connect();
						    if (robot_con.getResponseCode() == 200) {
						    	BufferedReader robot_r = new BufferedReader(new InputStreamReader(robot_con.getInputStream()));
					    		String robots_txt = "";
					    		while (true) {
					    			String l = robot_r.readLine();
					    			if (l == null)
					    				break;
					    			robots_txt = robots_txt + "\n" + l;
					    		}
					    		kvs.put("hosts", parsed_url[1], "robots_txt", robots_txt);
						    }
				    	}
				    	kvs.put("hosts", parsed_url[1], "last_accessed_time", String.valueOf(System.currentTimeMillis()));
						HttpURLConnection con = (HttpURLConnection)(new URL(u)).openConnection();
					    con.setRequestMethod("HEAD");
					    con.setDoOutput(true);
					    con.setInstanceFollowRedirects(false);
					    con.setRequestProperty("User-Agent", "cis5550-crawler");
					    con.connect();
					    
					    Row row = new Row(String.valueOf(u.hashCode()));
					    row.put("url", u);
					    int code = con.getResponseCode();
					    row.put("responseCode", String.valueOf(code));
					    Map<String, List<String>> header = con.getHeaderFields();
					    Map<String, String> lower_header = new HashMap<String, String>();
					    for (String key: header.keySet()) {
					    	if (key != null && header.get(key).size() > 0) {
					    		lower_header.put(key.toLowerCase(), header.get(key).get(0));
					    	}
					    }
					    if (lower_header.containsKey("content-type")) {
					    	row.put("contentType", lower_header.get("content-type").toString());
					    }
					    if (lower_header.containsKey("content-length")) {
					    	row.put("length", con.getHeaderField("Content-Length"));
					    }
					    HashSet<Integer> s = new HashSet<Integer>(Arrays.asList(301, 302, 303, 307, 308));
					    if (code == 200) {
					    	HttpURLConnection con1 = (HttpURLConnection)(new URL(u)).openConnection();
						    con1.setRequestMethod("GET");
						    con1.setDoOutput(true);
						    con1.setRequestProperty("User-Agent", "cis5550-crawler");
						    con1.setInstanceFollowRedirects(false);
						    con1.connect();
						    InputStream in = con1.getInputStream();
						    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
						    
						    int b;
						    while ((b = in.read()) != -1) {
					    		buffer.write(b);
					    	}
						    String result = new String(buffer.toByteArray());
						    for (String line: result.split("\n")) {
						    	String new_raw_url = find_url(line);
					    		String new_url = normalize(new_raw_url, u);
					    		if (!new_raw_url.equals("") && !new_url.equals("")) {
					    			new_url_list.add(new_url);
					    		}
					    		if (new_url.contains("c8Rhi6R/LZ5suxK8h9.html")
					    				|| new_url.contains("aVzUge/sn90Ztx2.html")
					    				|| new_url.contains("c8Rhi6R/fq49KSVyY1aJoR.html")
					    				|| new_url.contains("aVzUge/sw1Nc7T0.html")) {
					    			System.out.println(line);
					    			System.out.println(new_raw_url);
					    			System.out.println(new_url);
					    		}
						    }

					    	row.put("responseCode", String.valueOf(con1.getResponseCode()));
					    	
					    	if (row.get("contentType") != null && row.get("contentType").equals("text/html")) {					    		
					    		row.put("page", result);
					    	}
					    } else if (s.contains(code)){
					    	System.out.println(u+" "+u.hashCode()+" "+ code);
					    	System.out.println(lower_header.get("location"));
					    	new_url_list.add(normalize(lower_header.get("location"), u));
					    }
					    kvs.putRow("crawl", row);

					    return new_url_list;
					    
					});
		    		Thread.sleep(100);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
