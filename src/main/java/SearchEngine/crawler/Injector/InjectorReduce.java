package SearchEngine.crawler.Injector;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 14-6-29.
 */

public class InjectorReduce extends Reducer<Text, url_data, Text, url_data> {

    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {

        url_data data = new url_data();

        for (url_data tmp : values) {
            if (tmp.getStatus() == url_data.STATUS_INJECTED) {
                data.set(tmp);
                data.setStatus(url_data.STATUS_DB_UNFETCHED);
                break;
            }
        }

        context.write(key, data);
    }
}
