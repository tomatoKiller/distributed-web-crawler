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
public class FetchReduce extends Reducer<Text, url_data, Text, url_data> {

    MultipleOutputs<Text,url_data>  nu;
//    MultipleOutputs cont;


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        nu.close();
//        cont.close();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        cont = new MultipleOutputs<Text, Text>(context);
        nu = new MultipleOutputs<Text,url_data>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {

//        long Segment = context.getCounter(Depth.SEGMENT).getValue();

        //将url对应的网页内容写入content 文件夹 ,实际上写的是url_data 结构
        nu.write("Content", key, values.iterator().next(), "Content/");


        //将key中url的内容清空，并和其他新生成的url一起放入crawlDB中，供下一轮使用
        values.iterator().next().setContent(new Text(""));
        nu.write("newUrl", key, values.iterator().next(), "newUrl/");


        LinkedList<String> urlList = RetrievePage.findUrl(key.toString(), values.iterator().next().getContent().toString());

        for (String u : urlList) {
            url_data data = new url_data();
            data.setStatus(url_data.STATUS_INJECTED);
            nu.write("newUrl", key, values.iterator().next(), "newUrl/");
        }


    }
}
