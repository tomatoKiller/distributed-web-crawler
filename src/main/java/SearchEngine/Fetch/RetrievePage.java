package SearchEngine.Fetch;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wu on 2014/7/2.
 */
public class RetrievePage {
    private HttpClient httpClient = new HttpClient();
    private String url;

    public String getContent() {
        return content;
    }

    private String content;

    public RetrievePage(String url) {
        this.url = url;
    }

    public boolean downloadPage() {

        HttpMethod method = new GetMethod(url);

        ArrayList<String> user_agents = new ArrayList<String>(Arrays.asList("Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11",
                "Opera/9.25 (Windows NT 5.1; U; en)",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
                "Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)",
                "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12",
                "Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9",
                "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
                "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 "));


        Random rand = new Random();
        method.setRequestHeader("User-agent", user_agents.get(rand.nextInt(user_agents.size())));
        method.setRequestHeader("Accept","*/*");
        method.setRequestHeader("Referer","http://www.google.com");

        try {

            httpClient.executeMethod(method);
            int statusCode = method.getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                content = method.getResponseBodyAsString();
            }
////            else if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY
////                || statusCode == HttpStatus.SC_SEE_OTHER || statusCode == HttpStatus.SC_TEMPORARY_REDIRECT) {
////            Header header = method.getResponseHeader("location");
////            if (header == null) {
////                return false;
////            } else {
////                url = header.getValue();
////                if (url == null || url.equals("")) {
////                    url = "/";
////                }
////                downloadPage();
////            }
//        }

        } catch (IOException e) {
            return false;
        } finally {
            method.releaseConnection();
        }

        return true;
    }

    public static String getEncode(String content) {

        Pattern pattern=Pattern.compile("content=\"[\\s\\S]*?charset=(\\S+?)\"");

        Matcher matcher=pattern.matcher(content);
        if(matcher.find()){
            return matcher.group(1);
        } else
            return null;

    }

    /**
     * URL检查<br>
     * <br>
     * @param pInput     要检查的字符串<br>
     * @return boolean   返回检查结果<br>
     */
    public static boolean isUrl (String pInput) {
        if(pInput == null || pInput == ""){
            return false;
        }

        String regEx = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]" ;

        Pattern p = Pattern.compile(regEx);
        Matcher matcher = p.matcher(pInput);
        return matcher.matches();
    }

    private static boolean filter(String url) {
        //将过滤规则应用在url上，如果该url应当被过滤，则返回false，否则返回true
        return true;
    }

    public static LinkedList<String> findUrl(String baseUrl, String content) {
        LinkedList<String> result = new LinkedList<String>();

        String code = getEncode(content);
        if (code == null)
            return new LinkedList<String>();
        else {
            Parser parse = new Parser();
            try {
                parse.setInputHTML(content);
                parse.setEncoding(parse.getEncoding());
                parse.getLexer().getPage().setBaseUrl(baseUrl);

                NodeList list = parse.parse(new NodeClassFilter(LinkTag.class));
                for (int i = 0; i < list.size(); i++) {
                    LinkTag n =(LinkTag) list.elementAt(i);

                    if (isUrl(n.getLink()) && filter(n.getLink()))
                        result.add(n.getLink());
                }

            } catch (ParserException e) {
                e.printStackTrace();
                return new LinkedList<String>();
            }
        }
        return result;
    }

}
