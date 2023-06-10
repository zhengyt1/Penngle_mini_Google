package cis5550.tools;

import java.util.List;
import java.util.stream.Stream;

import static cis5550.tools.URLParser.parseURL;

public class Helper {

    public static String normalizeURL(String url) {
        String[] urlComponents = parseURL(url);
        if (urlComponents[2] == null) {
            if (urlComponents[0].equals("https")) {
                urlComponents[2] = "443";
            } else {
                urlComponents[2] = "80";
            }
        }
        return normalizeURL(urlComponents);
    }

    public static String normalizeURL(String[] urlComponents) {
        if (urlComponents[2] == null) {
            if (urlComponents[0].equals("https")) {
                urlComponents[2] = "443";
            } else {
                urlComponents[2] = "80";
            }
        }

        return urlComponents[0] + "://" + urlComponents[1]
                + ":" + urlComponents[2] + urlComponents[3];
    }

    public static String joinPath(String src, String dest) {
        dest = dest.replaceAll("#.*$", "");
        if (dest.isEmpty()) return src;
        if (dest.startsWith("/")) return dest;
        if (dest.startsWith(".")) {
            List<String> srcList = Stream.of(src.split("/")).filter(s -> !s.isEmpty()).toList();
            List<String> destList = Stream.of(dest.split("/")).filter(s -> !s.isEmpty()).toList();
            int destStart = 0, srcEnd = srcList.size() - (src.endsWith("/") ? 0 : 1);
            for (String d : destList) {
                if (d.equals(".")) {
                    destStart++;
                } else if (d.equals("..")) {
                    destStart++;
                    if (srcEnd > 0) {
                        srcEnd--;
                    }
                } else {
                    break;
                }
            }

            String srcStr = String.join("/", srcList.subList(0, srcEnd));
            String destStr = String.join("/", destList.subList(destStart, destList.size()));
            return "/" + (srcStr.isEmpty() ? "" : srcStr + "/") + destStr;
        } else {
            return src.substring(0, src.lastIndexOf('/') + 1) + dest;
        }
    }

    public static boolean startWithIgnoredProtocol(String url) {
        String[] urlComponents = parseURL(url);
        return !(urlComponents[0] == null || urlComponents[0].equals("http") || urlComponents[0].equals("https"));
    }
}