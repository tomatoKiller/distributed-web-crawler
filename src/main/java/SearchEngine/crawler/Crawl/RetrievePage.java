package SearchEngine.crawler.Crawl;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;

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







}
