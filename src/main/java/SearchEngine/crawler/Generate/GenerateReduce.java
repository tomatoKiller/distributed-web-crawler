package SearchEngine.crawler.Generate;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/6/29.
 */
public class GenerateReduce extends Reducer<Text, url_data, Text, url_data> {

    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {
        long timearg = 0;
        url_data newdata = new url_data();
        for (url_data val : values) {
            if (val.getLastFetchTime() > timearg) {
                timearg = val.getLastFetchTime();
                newdata.set(val);
            }
        }

        if (timearg != 0)
            context.write(key, newdata);
    }
}
