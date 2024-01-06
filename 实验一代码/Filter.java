package cn.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Filter {

    public static final class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 分割操作与在Sample相同
            String line = value.toString();
            String[] list = line.split("\\|");
            // 定义两个double变量存储longitude和latitude
            double longitude = Double.parseDouble(list[1]);
            double latitude = Double.parseDouble(list[2]);
            // 只保留对应属性取值在有效范围内的数据
            if (longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511) {
                context.write(value, new Text(""));
            }

        }
    }


    public static final class FilterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }

    // main函数几乎都相同
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 定义文件输入输出路径
        String input = "C:\\Users\\LiuJunshan\\Desktop\\D_Sample\\1.txt";
        String out = "C:\\Users\\LiuJunshan\\Desktop\\D_Filter";
        // 创建conf和job
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf,"Filter");
        // 设置jar包所在路径
        job.setJarByClass(Filter.class);
        // 指定Mapper类和Reducer类
        job.setMapperClass(FilterMapper.class);
        //job.setReducerClass(FilterReducer.class);
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