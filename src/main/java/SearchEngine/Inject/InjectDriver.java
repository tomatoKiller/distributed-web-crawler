package SearchEngine.Inject;

import SearchEngine.DataStructure.url_data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by wu on 2014/7/4.
 */
public class InjectDriver {
    public static Job Inject() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Inject");

        job.setJarByClass(InjectDriver.class);

        job.setMapperClass(InjectMap.class);
        job.setReducerClass(InjectReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(url_data.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(url_data.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/EngineSearch/InjectorInput"));
        FileOutputFormat.setOutputPath(job, new Path("/EngineSearch/CrawlDB/Round1"));

        job.waitForCompletion(true);

        return job;

    }
}
