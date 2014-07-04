package SearchEngine.Update;

import SearchEngine.DataStructure.url_data;
import SearchEngine.Generate.GenerateCombine;
import SearchEngine.Generate.GenerateMap;
import SearchEngine.Generate.GenerateReduce;
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
public class UpdateDriver {
    public static Job Update(int r) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Generate");

        job.setJarByClass(UpdateDriver.class);

        job.setMapperClass(UpdateMap.class);
        job.setCombinerClass(GenerateCombine.class);
        job.setReducerClass(UpdateReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(url_data.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(url_data.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/EngineSearch/FetchOut/Segment" + r + "/newUrl"));

        if (FileSystem.get(conf).exists(new Path("/EngineSearch/CrawlDB/Round" + (r + 1))))
            FileSystem.get(conf).delete(new Path("/EngineSearch/CrawlDB/Round" + (r + 1)), true);
        FileOutputFormat.setOutputPath(job, new Path("/EngineSearch/CrawlDB/Round" + (r + 1)));

        job.waitForCompletion(true);

        return job;

    }
}
