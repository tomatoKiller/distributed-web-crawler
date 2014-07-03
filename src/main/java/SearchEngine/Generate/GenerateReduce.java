package SearchEngine.Generate;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */
public class GenerateReduce extends Reducer<Text, url_data, Text, url_data> {
    //抽取未抓取的url，放入fetchlist
    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {

        for (url_data u : values) {
            if (u.getStatus() == url_data.STATUS_DB_FETCHED){
                return;
            }
        }

        context.write(key, values.iterator().next());
    }
}
