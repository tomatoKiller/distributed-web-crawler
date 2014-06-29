package SearchEngine.crawler.Generate;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/6/29.
 */
public class GenerateMap extends Mapper<Text, url_data, Text, url_data> {

    @Override
    protected void map(Text key, url_data value, Context context) throws IOException, InterruptedException {
        if (value.getStatus() == url_data.STATUS_DB_UNFETCHED) {
            value.setLastFetchTime(System.currentTimeMillis());
            context.write(key, value);
        }
        else if (value.getStatus() == url_data.STATUS_DB_FETCHED) {
            long updatetime = value.getLastFetchTime() + value.getFetchInterval();
            if (updatetime < System.currentTimeMillis()) {
                value.setStatus(url_data.STATUS_DB_UNFETCHED);
                value.setLastFetchTime(System.currentTimeMillis());
                context.write(key, value);
            }
        }

    }
}
