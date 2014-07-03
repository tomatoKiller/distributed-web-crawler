package SearchEngine.crawler.Crawl;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/7/2.
 */
public class CrawlMap extends Mapper<Text, Text, Text, BooleanWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        String tmp[] = text.split("\t");

        //输出格式  <url， 是否已经抓取过>
        context.write(new Text(tmp[0]), new BooleanWritable(Boolean.parseBoolean(tmp[1])));
    }
}
