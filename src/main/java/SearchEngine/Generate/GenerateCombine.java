package SearchEngine.Generate;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */
public class GenerateCombine extends Reducer<Text, url_data, Text, url_data> {
    //本地去重
    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {

        url_data data = new url_data();
        for (url_data u : values) {
            if (u.getStatus() == url_data.STATUS_DB_FETCHED || u.getStatus() == url_data.STATUS_FETCH_ERROR){
                data.set(u);
                context.write(key, data);
                return;
            }
        }

        data.set(values.iterator().next());
        data.setStatus(url_data.STATUS_DB_UNFETCHED);
        context.write(key, data);
    }
}
