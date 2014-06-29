package SearchEngine.crawler.Injector;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 14-6-29.
 */
public class InjectorDriven extends Mapper<Object, Text, Text, url_data> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String url = value.toString();
        if (url != null) {
            url_data data = new url_data(url_data.STATUS_INJECTED);
            context.write(value, data);
        }
    }
}
