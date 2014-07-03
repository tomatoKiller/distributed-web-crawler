package SearchEngine.crawler.Crawl;

import SearchEngine.DataStructure.DocumentWritable;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/2.
 */
public class CrawlReduce extends Reducer<Text, BooleanWritable, Text, DocumentWritable> {

    @Override
    protected void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {

        // 此处应先进性url格式检查
        String url = key.toString();


        if (!values.iterator().next().get()) {  //如果key所对应的url尚未抓取过，则进行下载
            //下载url对应的网页
            //此处应采用多线程。。。。。。。。。。。。。。。。
            RetrievePage tool = new RetrievePage(url);
            if (tool.downloadPage()) {
                context.write(key, new DocumentWritable(tool.getContent(), System.currentTimeMillis()));
            } else { //如果抓取失败，则放弃抓取，并将url标记为已抓取，防止在此抓取
                context.write(key, new DocumentWritable(null, 0));
            }
        } else {    //如果key对应的url已被抓取过，则不再进行抓取
            context.write(key, new DocumentWritable(null, 0));
        }
    }
}
