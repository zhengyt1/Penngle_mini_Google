package cis5550.tools;
import java.util.Base64;

public class Base64Tool {

    public static String encode(String str) {
        return Base64.getEncoder().encodeToString(str.getBytes());
    }

    public static String decode(String str) {
        return new String(Base64.getDecoder().decode(str));
    }
}
