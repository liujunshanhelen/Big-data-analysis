package cn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class driver1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new driver1(), args);
        System.exit(status);


    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), driver1.class.getSimpleName());
        job.setJarByClass(driver1.class);
        job.setMapperClass(wordcountmapper.class);
        job.setReducerClass(wordcountreduce.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        return job.waitForCompletion(true)?0:1;
    }
}
