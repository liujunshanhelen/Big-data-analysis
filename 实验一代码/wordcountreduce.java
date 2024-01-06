package cn.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class wordcountreduce extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable outvalue=new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long count=0;
        for (LongWritable value : values) {
            count+=value.get();
        }
        outvalue.set(count);
        context.write(key,outvalue);
    }
}
