package SearchEngine.Fetch;

import SearchEngine.DataStructure.DocumentWritable;
import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */
public class FetchMap extends Mapper<Text, url_data, Text, url_data> {
    @Override
    protected void map(Text key, url_data value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
