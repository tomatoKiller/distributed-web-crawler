package SearchEngine.Update;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/4.
 */
public class UpdateReduce  extends Reducer<Text, url_data, Text, url_data> {

    //去重，并写入CrawlDB，进行下一轮爬取
    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {

        for (url_data u : values) {
            if (u.getStatus() == url_data.STATUS_DB_FETCHED || u.getStatus() == url_data.STATUS_FETCH_ERROR) {
                context.write(key, u);
                return;
            }
        }

        context.write(key, values.iterator().next());
    }
}
