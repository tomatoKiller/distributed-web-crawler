package SearchEngine.crawler.Crawl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/2.
 */
public class CrawlCombine extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //Combine 用来在本地消除重复的url，减轻Reduce的压力
        context.write(key, values.iterator().next());

    }
}
