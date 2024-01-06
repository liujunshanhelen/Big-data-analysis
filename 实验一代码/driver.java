package cn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf, driver.class.getSimpleName());
        job.setJarByClass(driver.class);
        job.setMapperClass(wordcountmapper.class);
        job.setReducerClass(wordcountreduce.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new  Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);



    }
}
