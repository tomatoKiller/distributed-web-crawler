package SearchEngine.Generate;

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
 * Created by wu on 2014/7/4.
 */
public class GenerateDriver {
    public static Job Generate(int r) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Generate");

        job.setJarByClass(GenerateDriver.class);

        job.setMapperClass(GenerateMap.class);
        job.setCombinerClass(GenerateCombine.class);
        job.setReducerClass(GenerateReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(url_data.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(url_data.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/EngineSearch/CrawlDB/Round" + r ));

        if (FileSystem.get(conf).exists(new Path("/EngineSearch/Fetchlist")))
            FileSystem.get(conf).delete(new Path("/EngineSearch/Fetchlist"), true);

        FileOutputFormat.setOutputPath(job, new Path("/EngineSearch/Fetchlist"));

//        job.waitForCompletion(true);

        return job;

    }
}
