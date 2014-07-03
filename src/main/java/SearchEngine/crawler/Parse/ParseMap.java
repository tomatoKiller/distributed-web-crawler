package SearchEngine.crawler.Parse;

import SearchEngine.DataStructure.DocumentWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wu on 2014/7/2.
 */
public class ParseMap extends Mapper<Text, DocumentWritable, Text, BooleanWritable> {

    @Override
    protected void map(Text key, DocumentWritable value, Context context) throws IOException, InterruptedException {
        /*
        从value中解析出来下一层url，对于key值对应的url，其value设为true，表示已经抓取过
        对于value变量，如果其content为空，则表示该url已经抓取过，否则以<url, false> 的格式emit

        Reducer 貌似不需要
         */
    }
}
