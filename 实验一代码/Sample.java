package cn.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sample {


    public static final class SampleMapper extends Mapper<LongWritable,Text, Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\\|");

            Text career = new Text();
            career.set(list[10]);
            // 相当于只是在每一行前边加了一个head：career
            context.write(career,value);

        }
    }

    // Reduce：根据不同的career自动分组，每一组按照固定的比例（概率）进行抽样
    public static final class SampleReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text item:values){
                // 产生1-50的随机数
                int num = (int)(Math.random()*50);
                // 抽样比例为1/5
                if(num<10){
                    context.write(item,new Text(""));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 定义文件输入输出路径
        String input = "C:\\Users\\LiuJunshan\\Desktop\\data.txt";
        String out = "C:\\Users\\LiuJunshan\\Desktop\\D_Sample";
        // 创建conf和job
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf,"Sample");
        // 设置jar包所在路径
        job.setJarByClass(Sample.class);
        // 指定Mapper类和Reducer类
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(SampleReducer.class);
        // 指定maptask输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 制定reducetask输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 指定mapreducer程序数据的输入输出路径
        Path inputPath = new Path(input);
        Path outputPath = new Path(out);
        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);
        // 提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }

}