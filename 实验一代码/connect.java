package cn.wordcount;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class connect {
    public static  String DateFormatConversion(String s) throws ParseException {
        // 匹配 "March 10,1979"
        SimpleDateFormat format1 = new SimpleDateFormat("MMMM d,yyyy", Locale.ENGLISH);
        // 匹配 ”1975-09-21“
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        // 匹配 “1997/12/01”
        SimpleDateFormat format3 = new SimpleDateFormat("yyyy/MM/dd");

        // 第1种格式：转化成第3种格式
        if(s.contains(",")){
            Date date = format1.parse(s);
            return format3.format(date);
        }
        // 第2种格式：转化成第3种格式
        else if(s.contains("-")){
            Date date = format2.parse(s);
            return format3.format(date);
        }
        // 第3种格式：直接输出
        else{
            return s;
        }
    }

    public static double max = Double.NEGATIVE_INFINITY;
    public static double min = Double.POSITIVE_INFINITY;
    public static final class Sample1Mapper extends Mapper<LongWritable,Text, Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // 切割出一个个单词
            // 注意|前要加转义字符
            String[] list = line.split("\\|");
            // career赋值成对应的值
            Text career = new Text();
            career.set(list[10]);


            if(!list[6].equals("?")){
                double rating = Double.parseDouble(list[6]);
                if(rating>max){
                    max = rating;
                }
                if(rating<min){
                    min = rating;
                }
            }
            if(list[5].contains("℉")){
                float temperature = Float.parseFloat(list[5].substring(0,list[5].length()-1));
                temperature = (temperature-32)/1.8f;
                String t = String.format("%.1f",temperature)+"℃";
                line = line.replace(list[5],t);
            }
            try {
                String review_date = DateFormatConversion(list[4]);
                String user_birthday = DateFormatConversion(list[8]);
                line = line.replace(list[4],review_date);
                line = line.replace(list[8],user_birthday);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            double longitude = Double.parseDouble(list[1]);
            double latitude = Double.parseDouble(list[2]);
            if (longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511) {
                context.write(career,new Text(line));
            }








        }
    }
    public static final class Sample2Reducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                String line = value.toString();
                // 切割出一个个单词
                // 注意|前要加转义字符
                String[] list = line.split("\\|");
                if(!list[6].equals("?")){

                    double rating = Double.parseDouble(list[6]);
                    // Min-Max归一化
                    rating = (rating-min)/(max-min);
                    line = line.replace(list[6],String.valueOf(rating));
                }
                int num = (int)(Math.random()*50);
                if(num<10){
                    context.write(new Text(line),new Text(""));
                }

            }

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 定义文件输入输出路径
        String input = "C:\\Users\\LiuJunshan\\Desktop\\data.txt";
        String out = "C:\\Users\\LiuJunshan\\Desktop\\D_Connect";
        // 创建conf和job
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf,"connect");
        // 设置jar包所在路径
        job.setJarByClass(connect.class);
        // 指定Mapper类和Reducer类
        job.setMapperClass(connect.Sample1Mapper.class);
        job.setReducerClass(connect.Sample2Reducer.class);
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





