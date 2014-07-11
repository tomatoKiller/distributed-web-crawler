package SearchEngine.Fetch;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by wu on 14-7-11.
 */
public class PartitionByHostName extends Partitioner<Text, url_data> {
    @Override
    public int getPartition(Text text, url_data url_data, int numPartitions) {
        //按照url的主机名进行分区
        String url = text.toString();

        //    形如http://***.***.***/**/****/......  之类的url，任意选择//之后开始的位置为开始，搜寻地一个/符号
        int index = url.indexOf('/', 9);
        String host = url.substring(0, index);
        int charsum = 0;

        for (char c : url.toCharArray()) {
            charsum += (int)c;
        }

        return charsum % numPartitions;
    }
}
