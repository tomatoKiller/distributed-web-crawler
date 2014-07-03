package SearchEngine.crawler.Crawl;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by wu on 2014/7/2.
 */
public class CrawlPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String url = text.toString();
        int start = url.indexOf("://");
        url = url.substring(start+3);
        int end = url.indexOf("/");
        url = url.substring(0,end);
        int tmp = 0;

        for (char c : url.toCharArray()) {
            tmp += (int)c;
        }

        return tmp % i;
    }
}
