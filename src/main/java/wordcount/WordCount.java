package wordcount;

import hdfs.HDFSClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    /***
     *  /**
     * Mapper区: WordCount程序 Map 类
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:
     *        |       |           |             |
     *  输入key类型  输入value类型      输出key类型 输出value类型
     */
    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // KEYOUT Mapper输出结果KEY
        private Text word = new Text();
        // VALUEOUT MAPPER输出结果Value
        private final static IntWritable one = new IntWritable(1);


        /***
         *
         * @param key  行号
         * @param value  是文本每一行的值
         * @param context  上下文对象
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString(); //获取每行数据
            // 分词 将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
            StringTokenizer tokenizer = new StringTokenizer(lineValue);
            // 遍历
            while (tokenizer.hasMoreTokens()){
                //获取每一个值
                String eachWord = tokenizer.nextToken();
//              设置 map 输出 key
                word.set(eachWord);
//              将结果输出到Context
                context.write(word, one);
            }

        }
    }



    /**
     * Reducer 区域：WordCount 程序 Reduce 类
     * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:Map 的输出类型，就是Reduce 的输入类型
     * @author johnnie
     *
     */
    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 输出结果：总次数
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;   // 累加器，累加每个单词出现的总次数
            // 遍历values
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 设置输出
            result.set(sum);
            // 上下文输出 reduce 结果
            context.write(key,result);
        }
    }

    public static void  main(String[] args) throws Exception{
        //输入路径
//        String inPath = "hdfs://hadoop-master:9000/user/root/input";
        String inPath = "input/1901";
        //输出路径，必须是不存在的，空文件加也不行。
//        String outPath = "hdfs://hadoop-master:9000/user/root/output1";
        String outPath = "out/1";
//        new HDFSClient().delete(outPath,true);
        // 获取配置信息
        Configuration conf = new Configuration();
        // 创建一个 Job
        Job job = Job.getInstance(conf, "word count");      // 设置 job name 为 word count
//      job = new Job(conf, "word count");                  // 过时的方式

        conf.set("fs.defaultFS","hdfs://hadoop-master:9000");
        // 1. 设置 Job 运行的类
        job.setJarByClass(WordCount.class);

        // 2. 设置Mapper类和Reducer类
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // 3. 获取输入参数，设置输入文件目录和输出文件目录
        FileInputFormat.setInputPaths(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        // 4. 设置输出结果 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//      job.setCombinerClass(IntSumReducer.class);

        // 5. 提交 job，等待运行结果，并在客户端显示运行信息，最后结束程序
        boolean isSuccess = job.waitForCompletion(true);

        System.out.println("OK");

        // 结束程序
        System.exit(isSuccess ? 0 : 1);
    }
}
