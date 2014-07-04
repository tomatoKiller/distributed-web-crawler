package SearchEngine.Update;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/7/4.
 */
public class UpdateMap  extends Mapper<Text, url_data, Text, url_data> {

    //Map在此处不进行特殊处理
    @Override
    protected void map(Text key, url_data value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
