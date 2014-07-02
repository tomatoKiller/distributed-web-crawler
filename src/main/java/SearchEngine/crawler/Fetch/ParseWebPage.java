package SearchEngine.crawler.Fetch;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import java.util.ArrayList;

/**
 * Created by IORI on 2014/7/2.
 */
public class ParseWebPage {
    private String content;
    private String url;
    private ArrayList<String> new_urls;

    public ParseWebPage(String url) {
        this.url = url;
    }

    private void parse(){
        //        try {
//            Parser parser = new Parser(url);
//            NodeList nodeList = parser.extractAllNodesThatMatch(new NodeFilter() {
//                @Override
//                public boolean accept(Node node) {
//                    if (node instanceof LinkTag)
//                        return true;
//                    return false;
//                }
//            });
//
//            for (int i = 0; i < nodeList.size(); i++) {
//
//            }
//        } catch (ParserException e) {
//            e.printStackTrace();
//        }

        /*  获取对应url的内容
            将其赋值给content成员变量
            并随后调用findUrl函数，提取网页中的url,存入urls容器
         */
        content = "";
        new_urls = new ArrayList<String>();
    }

    public String getContent(String url) {
        return content;
    }


}
