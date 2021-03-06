package SearchEngine.Fetch;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by wu on 2014/7/3.
 */
public class FetchReduce extends Reducer<Text, url_data, Text, url_data> {

    MultipleOutputs<Text,url_data>  nu;


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        nu.close();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nu = new MultipleOutputs<Text,url_data>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<url_data> values, Context context) throws IOException, InterruptedException {



        LinkedList<String> urlList;

        String url = key.toString();


        //下载url对应的网页
        RetrievePage tool = new RetrievePage(url);
        url_data data = new url_data();
        data.set(values.iterator().next());

        if (tool.downloadPage()) {
            Text content = new Text(tool.getContent());
            data.setContent(content);
            data.setLastFetchTime(System.currentTimeMillis());
            data.setStatus(url_data.STATUS_DB_FETCHED);
        } else { //如果抓取失败，则放弃抓取，并将url标记为已抓取，防止在此抓取
            data.setLastFetchTime(System.currentTimeMillis());
            data.setStatus(url_data.STATUS_FETCH_ERROR);
        }



        urlList = RetrievePage.findUrl(key.toString(), data.getContent().toString());

        //将url对应的网页内容写入content 文件夹 ,实际上写的是url_data 结构
        nu.write("Content", key, data, "Content/" + getMD5(key.toString().getBytes()) + "/");

        //将key中url的内容清空，并和其他新生成的url一起放入crawlDB中，供下一轮使用
        data.setContent(new Text(""));
        nu.write("newUrl", key, data, "newUrl/");


        for (String u : urlList) {
            url_data dat = new url_data();
            dat.setStatus(url_data.STATUS_INJECTED);
            nu.write("newUrl", new Text(u), dat, "newUrl/");
        }


    }

    public static String getMD5(byte[] source) {
        String s = null;
        char hexDigits[] = {       // 用来将字节转换成 16 进制表示的字符
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',  'e', 'f'};
        try
        {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance( "MD5" );
            md.update( source );
            byte tmp[] = md.digest();          // MD5 的计算结果是一个 128 位的长整数，
            // 用字节表示就是 16 个字节
            char str[] = new char[16 * 2];   // 每个字节用 16 进制表示的话，使用两个字符，
            // 所以表示成 16 进制需要 32 个字符
            int k = 0;                                // 表示转换结果中对应的字符位置
            for (int i = 0; i < 16; i++) {          // 从第一个字节开始，对 MD5 的每一个字节
                // 转换成 16 进制字符的转换
                byte byte0 = tmp[i];                 // 取第 i 个字节
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];  // 取字节中高 4 位的数字转换,
                // >>> 为逻辑右移，将符号位一起右移
                str[k++] = hexDigits[byte0 & 0xf];            // 取字节中低 4 位的数字转换
            }
            s = new String(str);                                 // 换后的结果转换为字符串

        }catch( Exception e )
        {
            e.printStackTrace();
        }
        return s;
    }

}
