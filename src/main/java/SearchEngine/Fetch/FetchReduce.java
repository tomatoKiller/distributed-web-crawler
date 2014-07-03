package SearchEngine.Fetch;

import SearchEngine.DataStructure.url_data;
import SearchEngine.JobDriver.Depth;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;

/**
 * Created by wu on 2014/7/3.
 */
public class FetchReduce extends Reducer<Text, url_data, Text, Text> {

    MultipleOutputs<Text, Text> mos;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {

        long Segment = context.getCounter(Depth.SEGMENT).getValue();

        //将url对应的网页内容写入content 文件夹
        mos.write("Content", key, values.iterator().next().getContent(), String.valueOf(Segment)+"/");
        //将key中url的内容清空，并和其他新生成的url一起放入crawlDB中，供下一轮使用
        values.iterator().next().setContent(new Text(""));
        mos.write("CrawlDB", key, values.iterator().next(), String.valueOf(Segment)+"/");


        LinkedList<String> urlList = RetrievePage.findUrl(key.toString(), values.iterator().next().getContent().toString());

        for (String u : urlList) {
            url_data data = new url_data();
            data.setStatus(url_data.STATUS_INJECTED);
            mos.write("CrawlDB", key, u, String.valueOf(Segment)+"/");
        }


    }
}
