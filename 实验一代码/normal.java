package cn.wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class normal {

    // 本类用于进行格式转换以及归一化处理
    // review_date：12个属性中的第5个
    // temperature：12个属性中的第6个
    // rating：12个属性中的第7个
    // user_birthday：12个属性中的第9个
    // 初始化max和min(全局变量)
    public static double max = Double.NEGATIVE_INFINITY;
    public static double min = Double.POSITIVE_INFINITY;

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


    public static final class FormatNormalizeMapper extends Mapper<LongWritable, Text, Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] list = line.split("\\|");
            // 1. 更新rating的最大最小值
            // 如果此时的rating不缺失
            if(!list[6].equals("?")){
                double rating = Double.parseDouble(list[6]);
                if(rating>max){
                    max = rating;
                }
                if(rating<min){
                    min = rating;
                }
            }

            // 2. 统一摄氏度和华氏度
            if(list[5].contains("℉")){
                float temperature = Float.parseFloat(list[5].substring(0,list[5].length()-1));
                temperature = (temperature-32)/1.8f;
                String t = String.format("%.1f",temperature)+"℃";
                line = line.replace(list[5],t);
            }

            // 3. 统一日期信息
            try {
                String review_date = DateFormatConversion(list[4]);
                String user_birthday = DateFormatConversion(list[8]);
                line = line.replace(list[4],review_date);
                line = line.replace(list[8],user_birthday);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            // 4. 对应字段输出到reduce进行进一步处理
            context.write(new Text(line),new Text(""));

        }
    }

    public static final class FormatNormalizeReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String[] list = line.split("\\|");
            // rating不缺失
            if(!list[6].equals("?")){
                double rating = Double.parseDouble(list[6]);
                // Min-Max归一化
                rating = (rating-min)/(max-min);
                line = line.replace(list[6],String.valueOf(rating));
            }


            for(Text value:values){
                context.write(new Text(line),new Text(""));
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 定义文件输入输出路径
        String input = "C:\\Users\\LiuJunshan\\Desktop\\D_Filter\\part-r-00000";
        String out = "C:\\Users\\LiuJunshan\\Desktop\\D_Filter_1";
        // 创建conf和job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FormatNormalize");
        // 设置jar包所在路径
        job.setJarByClass(normal.class);
        // 指定Mapper类和Reducer类
        job.setMapperClass(FormatNormalizeMapper.class);
        job.setReducerClass(FormatNormalizeReducer.class);
        // 指定maptask输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 制定reducetask输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 指定mapreducer程序数据的输入输出路径
        Path inputPath = new Path(input);
        Path outputPath = new Path(out);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        // 提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : 1);
    }

}