package cn.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordcountmapper extends Mapper<LongWritable, Text,Text,LongWritable>{

    private Text outkey = new Text();
    private final static LongWritable outvalue = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] words = line.split("\\s+");
        for (String word : words) {
            outkey.set(word);
            context.write(outkey,outvalue);


        }

    }
}
