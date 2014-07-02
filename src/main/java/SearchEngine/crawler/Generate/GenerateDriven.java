package SearchEngine.crawler.Generate;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by wu on 2014/6/29.
 */
public class GenerateDriven {

    public static boolean generate() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Generate");

        // /SearchEngine/inject 作为Injector模块的输出，Generate模块的输入
        FileInputFormat.addInputPath(job, new Path("/SearchEngine/CrawlDB"));

        Path GenerateOutput = new Path("/SearchEngine/fetchList");
        if (FileSystem.get(conf).exists(GenerateOutput)) {
            FileSystem.get(conf).delete(GenerateOutput, true);
        }
        FileOutputFormat.setOutputPath(job, GenerateOutput);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(url_data.class);

        job.setMapperClass(GenerateMap.class);
        job.setReducerClass(GenerateReduce.class);

        //返回执行结果的状态
        return job.waitForCompletion(true);
    }
}
