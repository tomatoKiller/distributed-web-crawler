package SearchEngine.crawler.Injector;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 14-6-29.
 */
public class InjectorMap  extends Mapper<Object, Text, Text, url_data> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //此处可采用normalizer，filter等插件对url数据进行格式过操作。。。。

        String url = value.toString();
        if (url != null) {
            url_data data = new url_data(url_data.STATUS_INJECTED);
            context.write(value, data);
        }
    }
}
