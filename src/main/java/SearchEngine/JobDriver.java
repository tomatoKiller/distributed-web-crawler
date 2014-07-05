package SearchEngine;

import SearchEngine.Fetch.FetchDriver;
import SearchEngine.Generate.GenerateDriver;
import SearchEngine.Inject.InjectDriver;
import SearchEngine.Update.UpdateDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

/**
 * Created by wu on 2014/7/3.
 */



public class JobDriver {

    public static void main(String[] args) throws Exception {
        int round = 0;

        Configuration conf = new Configuration();

        int depth = conf.getInt("depth", 10);

        JobControl jobcntl = new JobControl("Crawler");

        ControlledJob inject = new ControlledJob(conf);
        inject.setJob(InjectDriver.Inject());

        while (round < depth) {

            ControlledJob generate = new ControlledJob(conf);
            generate.setJob(GenerateDriver.Generate(round));
            generate.addDependingJob(inject);

            ControlledJob fetch = new ControlledJob(conf);
            fetch.setJob(FetchDriver.Fetch(round));
            fetch.addDependingJob(generate);

            ControlledJob update = new ControlledJob(conf);
            update.setJob(UpdateDriver.Update(round));
            update.addDependingJob(fetch);

            jobcntl.addJob(inject);
            jobcntl.addJob(generate);
            jobcntl.addJob(fetch);
            jobcntl.addJob(update);

            Thread thread = new Thread(jobcntl);
            thread.start();

            while (true) {
                if (jobcntl.allFinished()) {
                    jobcntl.stop();
                    break;
                } else if (jobcntl.getFailedJobList().size() > 0) {
                    throw new RuntimeException("some jobs failed");
                }
            }

            ++round;

        }
    }

}
