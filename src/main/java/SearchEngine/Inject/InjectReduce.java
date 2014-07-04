package SearchEngine.Inject;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */

public class InjectReduce extends Reducer<Text, url_data, Text, url_data> {

    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
