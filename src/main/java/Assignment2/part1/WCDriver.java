package Assignment2.part1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WCDriver {
    public static void main(String args[]) throws Exception {
        Configuration configuration = new Configuration();
        URI uri = new URI("hdfs://localhost:9000");
        FileSystem hdfs = FileSystem.get(uri,configuration);

        Job job = Job.getInstance(configuration,"WC");
        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/Users/mdhasnain/Test/people1.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output1"));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
