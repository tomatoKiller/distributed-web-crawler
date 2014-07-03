package SearchEngine.Inject;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */
public class InjectMap extends Mapper<Object, Text, Text, url_data> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        String url = value.toString();
        url_data data = new url_data();
        data.setStatus(url_data.STATUS_INJECTED);
        context.write(value, data);
    }
}
