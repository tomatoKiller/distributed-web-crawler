package SearchEngine.crawler.Parse;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wu on 2014/7/2.
 */
public class ParseCombine extends Reducer<Text, BooleanWritable, Text, BooleanWritable> {

    @Override
    protected void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
        for (BooleanWritable b : values) {
            boolean tmp = b.get();
            if (tmp == true) {
                context.write(key, new BooleanWritable(true));
                return;
            }
        }
        context.write(key, new BooleanWritable(false));
    }
}
