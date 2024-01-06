package project2;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class result {
    static String DATA_PATH = "C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\测试数据.txt";
    static String Classified_Statistics = "C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\Classified_Statistics\\part-r-00000";
    //	static String Classified_Result = "hdfs://localhost:9000/output/experiment2/Classified_Result";
    static String Classified_Result = "C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\Classified_Result3";

    public static int dim = 20;			// 数据维度为20



    public static final class NaiveBayesClassificationMapper extends Mapper<Object, Text, Text, Text> {

        // 通过连续属性离散化的转换规则，转化出新的属性序列
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException {


            String[] items = value.toString().split(",");
            String newSequence = "";
            for(int i = 0; i < dim; i++) {
                double itemNum = Double.valueOf(items[i]);
                if(itemNum > 0) {
                    newSequence = newSequence + 1 + ",";
                } else {
                    newSequence = newSequence + 0 + ",";
                }
            }

            context.write(new Text(newSequence), value);
        }

    }

    public static final class NaiveBayesClassificationReducer extends Reducer<Text, Text, Text, Text> {

        public static table probabilityTable;

        public static int count = 0;
        public static int right = 0;

        protected void setup(Context context) throws IOException {
            probabilityTable = new table(Classified_Statistics);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int selectedClass = 0;
            double maxProbability = 0.0;
            double[] classProbability = {1.0, 1.0};

            String[] items = key.toString().split(",");
            int[] itemsNum = new int[20];

            for(int i = 0; i < 20; i++) {
                itemsNum[i] = Integer.valueOf(items[i]);
            }
            for(int i = 0; i < 2; i++) {
                for(int j = 0; j < 20; j++) {
                    classProbability[i] = classProbability[i] *
                            probabilityTable.getConditionalProbability(j, itemsNum[j], i) /
                            probabilityTable.getTypeProbability(j, itemsNum[j]);
                }
                classProbability[i] *= probabilityTable.getClassProbability(i);
            }

            for(int i = 0; i < 2; i++) {
                if(classProbability[i] > maxProbability) {
                    maxProbability = classProbability[i];
                    selectedClass = i;
                }
            }

            for(Text value : values) {


                String result = value.toString() + "," + selectedClass;
                context.write(new Text(result), new Text(""));
            }

        }


    }

    public static void main(String[] arg) throws Exception{

        Path outputpath=new Path(Classified_Result);
        Path inputpath=new Path(DATA_PATH);
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf, "BayesClassification");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setJarByClass(classic.class);
        job.setMapperClass(NaiveBayesClassificationMapper.class);
        job.setReducerClass(NaiveBayesClassificationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



