package SearchEngine;

import SearchEngine.Fetch.RetrievePage;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {

//        System.out.println(RetrievePage.isUrl("http://www.baidu. com"));

        RetrievePage tool = new RetrievePage("http://www.qq.com/");
        if (tool.downloadPage()) {
            System.out.println(tool.getContent());
            List<String> url = RetrievePage.findUrl("http://www.qq.com/", tool.getContent());
            System.out.println(url);
        }


//        HttpClient httpClient = new HttpClient();
//        HttpMethod getmethod = new GetMethod("http://blog.csdn.net/wisgood/article/details/8797251");
//
//        ArrayList<String> user_agents = new ArrayList<String>(Arrays.asList("Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11",
//                "Opera/9.25 (Windows NT 5.1; U; en)",
//                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
//                "Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)",
//                "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12",
//                "Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9",
//                "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
//                "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 "));
////        ArrayList<Integer> aaa = new ArrayList<Integer>(Arrays.asList(1,2,3));
//
//        Random rand = new Random();
//        getmethod.setRequestHeader("User-agent", user_agents.get(rand.nextInt(user_agents.size())));
//        getmethod.setRequestHeader("Accept","*/*");
//        getmethod.setRequestHeader("Referer","http://www.google.com");
//
//        httpClient.executeMethod(getmethod);
//        String content = getmethod.getResponseBodyAsString();
//
//        Parser parse = new Parser();
//        try {
//            parse.setInputHTML(content);
//            parse.setEncoding(parse.getEncoding());
//            parse.getLexer().getPage().setBaseUrl("http://blog.csdn.net/wisgood/article/details/8797251");
//
//            NodeList list = parse.parse(new NodeClassFilter(LinkTag.class));
//            for (int i = 0; i < list.size(); i++) {
//                LinkTag n =(LinkTag) list.elementAt(i);
//
//                if (RetrievePage.isUrl(n.getLink()))
//                    System.out.println(n.getLink());
//            }
//
//        } catch (ParserException e) {
//            e.printStackTrace();
//        }


//        System.out.println(content);
////        String regex="^([\\s\\S]*)(<meta\\s+http-equiv=\"Content-Type\"\\s+content=\"text/html;\\s*charset=)(\\w+)(\">[\\s\\S]*)$";
//        Pattern pattern=Pattern.compile("content=\"[\\s\\S]*?charset=(\\S+?)\"");
////        Pattern pattern=Pattern.compile("content=\"([\\s\\S]*?)charset=(\\S+?)\"");
//        Matcher matcher=pattern.matcher(content);
//        if(matcher.find()){
//            System.out.println(matcher.group(1));
////            System.out.println("OK");
//        }
    }
}
