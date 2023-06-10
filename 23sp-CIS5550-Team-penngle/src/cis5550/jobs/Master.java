package cis5550.jobs;

import static cis5550.webserver.Server.*;
import static cis5550.jobs.Query.*;


// import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;



public class Master extends cis5550.generic.Master {

  public static void main(String args[]) {

    port(Integer.parseInt(args[0]));
    registerRoutes();

    get("/query", (req, res) -> {
      String query = req.queryParams("query");
      String result = Query.queryThings(query);
      return result;
    });

    get("/query/:url", (req, res) -> {
      String hashurl = req.params("url");
      // String hashurl = Hasher.hash(url);
      KVSClient kvs = new KVSClient("localhost:8000");
      try {
        Map<String, String> result = new HashMap<>();
        Row resultRow = kvs.getRow("plain_text", hashurl);
        result.put("description", resultRow.get("description"));
        result.put("title", resultRow.get("head"));
        result.put("url", resultRow.get("url"));
        // Gson gson = new Gson();
        // String json = gson.toJson(data);
        String jsonString = toJsonString(result);
        // System.out.println(jsonString);
        return jsonString;
        // return {new String(descrip_result), new String(head_result)};
      } catch (IOException e) {
        System.out.println("error reading ");
      }
      return null;
    });

    get("/", (req, res) -> {
      byte[] encoded = Files.readAllBytes(Paths.get("/home/ec2-user/23sp-CIS5550-Team-penngle/src/Sample.html"));
      return new String(encoded);
    });
    get("/Search.js", (req, res) -> {
      byte[] encoded = Files.readAllBytes(Paths.get("/home/ec2-user/23sp-CIS5550-Team-penngle/src/Search.js"));
      return new String(encoded);
    });

  }

  // head: title ,
  // description: summary ,
  // text: html,
  // url:url

  private static String toJsonString(Map<String, String> data) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean first = true;
    for (Map.Entry<String, String> entry : data.entrySet()) {
      if (!first) {
        sb.append(",");
      }
      sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
      first = false;
    }
    sb.append("}");
    return sb.toString();
  }
}
