package SearchEngine.crawler.Injector;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by wu on 14-6-29.
 */
public class InjectorDriven{

    public static boolean inject() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Injector");

        String currentPath = System.getProperty("user.dir");

        //设定从用户给定的url文件中读取原始的种子url
        FileInputFormat.addInputPath(job, new Path(currentPath + "/datafile/InjectorInput/"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(url_data.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // /SearchEngine/inject 作为Injector模块的输出，Generate模块的输入
        Path InjectorOutput = new Path("/SearchEngine/inject");

        //在下次迭代中，在执行job前要先删除上次产生的输出
        if (FileSystem.get(conf).exists(InjectorOutput)) {
            FileSystem.get(conf).delete(InjectorOutput, true);
        }

        FileOutputFormat.setOutputPath(job, InjectorOutput);

        job.setMapperClass(InjectorMap.class);
        //尚未发现reduce在此处的用处，是否可以采用默认设置？
        job.setReducerClass(InjectorReduce.class);

        //返回执行结果的状态
        return job.waitForCompletion(true);

    }

}
