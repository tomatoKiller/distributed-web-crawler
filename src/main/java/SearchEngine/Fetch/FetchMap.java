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
        String url = key.toString();


        //下载url对应的网页
        //此处应采用多线程。。。。。。。。。。。。。。。。
        RetrievePage tool = new RetrievePage(url);
        url_data data = new url_data();

        if (tool.downloadPage()) {
            Text content = new Text(tool.getContent());
            data.setContent(content);
            data.setLastFetchTime(System.currentTimeMillis());
            data.setStatus(url_data.STATUS_DB_FETCHED);
            context.write(key, data);
        } else { //如果抓取失败，则放弃抓取，并将url标记为已抓取，防止在此抓取
            data.setLastFetchTime(System.currentTimeMillis());
            data.setStatus(url_data.STATUS_FETCH_ERROR);
            context.write(key, data);
        }
    }
}
