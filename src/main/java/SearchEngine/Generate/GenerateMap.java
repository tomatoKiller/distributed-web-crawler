package SearchEngine.Generate;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */
public class GenerateMap extends Mapper<Text, url_data, Text, url_data> {


    //从CrawlDB中提取url并进行格式检验
    @Override
    protected void map(Text key, url_data value, Context context) throws IOException, InterruptedException {
        if (value.getStatus() == url_data.STATUS_DB_FETCHED || value.getStatus() == url_data.STATUS_FETCH_ERROR) { //表示以前已经检查过该url，无需再次检查
            context.write(key, value);
        } else {
            //进行格式检查
            context.write(key, value);
        }
    }
}
