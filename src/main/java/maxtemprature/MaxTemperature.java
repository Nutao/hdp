package maxtemprature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;


public class MaxTemperature {
    public static void main(String[] args) throws Exception {

        //输入路径
        String inPath = "hdfs://hadoop-master:9000/user/root/input/1901";
        //输出路径，必须是不存在的，空文件加也不行。
        String outPath = "hdfs://hadoop-master:9000/user/root/output";

        // 获取配置信息
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker","hadoop-master:9001");
        conf.set("fs.default.name","hdfs://hadoop-master:9000");
        // 创建一个 Job
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(IntWritable.class);

        boolean isSuccess = job.waitForCompletion(true);

        System.out.println("OK");
        // 结束程序
        getNodeHost(conf);
        System.exit(isSuccess ? 0 : 1);
    }


    public static void getNodeHost(Configuration configuration) throws Exception{

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-master:9000"),configuration,"root");
        DistributedFileSystem hdfs = (DistributedFileSystem)fileSystem;

        DatanodeInfo[] datanodeInfos = hdfs.getDataNodeStats();

        for (DatanodeInfo info: datanodeInfos) {
            System.out.println("SlaveInfo Name"+ info.getHostName() +", ip"+info.getDatanodeReport());
        }
    }
}