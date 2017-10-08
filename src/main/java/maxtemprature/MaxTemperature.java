package maxtemprature;

import hdfs.HDFSClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;


public class MaxTemperature {
    public static void main(String[] args) throws Exception {

        //输入路径
        String inPath = "hdfs://hadoop-master:9000/user/root/input/1901";
        //输出路径，必须是不存在的，空文件加也不行。
        String outPath = "hdfs://hadoop-master:9000/user/root/output1";
        new HDFSClient().delete(outPath,true);

//        // 设置配置信息, 提交到Yarn
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker","hadoop-master:9001");
        conf.set("fs.defaultFS","hdfs://hadoop-master:9000");
//        //下面为了远程提交添加设置：
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.resource-tracker.address", "hadoop-master:8031");
        conf.set("yarn.resourcemanager.address", "hadoop-master:8032");
        conf.set("yarn.resourcemanager.scheduler.address", "hadoop-master:8030");
        conf.set("yarn.resourcemanager.admin.address", "hadoop-master:8033");
        conf.set("mapreduce.jobhistory.address", "hadoop-master:10020");
        conf.set("mapreduce.jobhistory.webapp.address", "hadoop-master:19888");
//        conf.set("mapred.child.java.opts", "-Xmx1024m");

        conf.addResource("hdpxml/core-site.xml");
        conf.addResource("hdpxml/hdfs-site.xml");
        conf.addResource("hdpxml/mapred-site.xml");
        conf.addResource("hdpxml/yarn-site.xml");


        Job job = Job.getInstance(conf);

        job.setJarByClass(MaxTemperature.class);
        job.setJobName("MaxTemperature");

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(IntWritable.class);


        job.submit(); // 提交到yarn
        boolean isSuccess = job.waitForCompletion(true);

        System.out.println("计算完成");
        System.out.println(job.getJobID().toString());
        // 结束程序
        System.exit(isSuccess ? 0 : 1);
    }
}