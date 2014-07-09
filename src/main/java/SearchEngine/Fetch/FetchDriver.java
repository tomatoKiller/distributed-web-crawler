package SearchEngine.Fetch;

import SearchEngine.DataStructure.url_data;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by wu on 2014/7/4.
 */
public class FetchDriver {
    public static Job Fetch(int r) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Fetch");

        job.setJarByClass(FetchDriver.class);

        job.setMapperClass(FetchMap.class);
        job.setReducerClass(FetchReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(url_data.class);

        //设置默认的输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/EngineSearch/Fetchlist"));

//        if (FileSystem.get(conf).exists(new Path("/EngineSearch/CrawlDB/FetchOut/Segment" + r)))
//            FileSystem.get(conf).delete(new Path("/EngineSearch/CrawlDB/FetchOut/Segment" + r), true);

        //设置默认的输出路径
        if (!FileSystem.get(conf).exists(new Path("/EngineSearch/UrlData/FetchOut")))
            FileSystem.get(conf).mkdirs(new Path("/EngineSearch/UrlData/FetchOut"));

        if (FileSystem.get(conf).exists(new Path("/EngineSearch/UrlData/FetchOut/Segment" + r)))
            FileSystem.get(conf).delete(new Path("/EngineSearch/UrlData/FetchOut/Segment" + r), true);

        FileOutputFormat.setOutputPath(job, new Path("/EngineSearch/UrlData/FetchOut/Segment" + r));

        MultipleOutputs.addNamedOutput(job, "newUrl", SequenceFileOutputFormat.class, Text.class, url_data.class);
        MultipleOutputs.addNamedOutput(job, "Content", SequenceFileOutputFormat.class, Text.class, url_data.class);


//        job.waitForCompletion(true);

        return job;

    }
}
