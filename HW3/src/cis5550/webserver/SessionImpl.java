package cis5550.webserver;

import java.util.HashMap;
import java.util.*;

public class SessionImpl implements Session {
//	String id;
//	long createTime;
//	long lastAccessTime;
//	int activeInterval;
	HashMap<String, Object> map = new HashMap<String, Object>();
	
	public SessionImpl() {
		String uniqueID = UUID.randomUUID().toString();
		attribute("id", uniqueID);
		attribute("createTime", System.currentTimeMillis());
		attribute("lastAccessTime", System.currentTimeMillis());
		maxActiveInterval(300);
	}
//	public SessionImpl(String id, long createTime, long lastAccessTime, int activeInterval) {
//		this.id = id;
//		this.createTime = createTime;
//		this.lastAccessTime = lastAccessTime;
//		this.activeInterval = activeInterval;
//	}
	// Returns the session ID (the value of the SessionID cookie) that this session is associated with
	public String id() {
		return (String) attribute("id");
	}

  // The methods below return the time this session was created, and the time time this session was
  // last accessed. The return values should be in the same format as the return value of 
  // System.currentTimeMillis().
    public long creationTime() {
    	return (long) attribute("createTime");
    }
    public long lastAccessedTime() {
    	return (long) attribute("lastAccessTime");
    }

  // Set the maximum time, in seconds, this session can be active without being accessed.
    public void maxActiveInterval(int seconds) {
    	attribute("activeInterval", (long) seconds * 1000);
    	attribute("isValid", (long) attribute("createTime") + (long) attribute("activeInterval") >= System.currentTimeMillis());
    }

  // Invalidates the session. You do not need to delete the cookie on the client when this method
  // is called; it is sufficient if the session object is removed from the server.
    public void invalidate() {
    	attribute("isValid", false);
    }

  // The methods below look up the value for a given key, and associate a key with a new value,
  // respectively.
    public Object attribute(String name) {
    	return map.get(name);
    }
    public void attribute(String name, Object value) {
    	map.put(name, value);
    }
}
