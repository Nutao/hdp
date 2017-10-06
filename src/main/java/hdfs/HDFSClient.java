package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {
    private FileSystem fs;

    /***
     * @param user 传入需要连接的用户
     * @throws Exception
     */
    public HDFSClient(String user) throws Exception {
        Configuration configuration = new Configuration();
//        HDFS 的地址需要修改成自己的地址
        configuration.set("fs.default.name","hdfs://hadoop-master:9000");
//        设定文件系统的URI, 配置, 以及用户
        fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"),configuration, user);
    }

    /***
     * 默认初始化root用户
     * @throws Exception
     */
    public HDFSClient() throws Exception {
        Configuration configuration = new Configuration();
//        HDFS 的地址需要修改成自己的地址
        configuration.set("fs.default.name","hdfs://hadoop-master:9000");
//        设定文件系统的URI, 配置, 以及用户
        fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"),configuration, "root");
    }

    /**
     * 删除HDFS 中指定的目录
     * @param path  需要删除的目录
     * @param is 是否进行递归删除文件
     * @throws IOException
     */
    public boolean delete(String path, boolean is) throws IOException {
        boolean res = true;
        if (fs.exists(new Path(path))) {
            res = fs.delete(new Path(path), is);
        }
        System.out.println("HDFS文件已删除");
        return res;
    }


    /***
     * 列出该路径下的所有文件
     * @param path
     * @throws IOException
     */
    public void list(String path) throws IOException{
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(path), true);
        while (files.hasNext()){
            LocatedFileStatus file = files.next();
            if (file.isFile()){
                System.out.println(file.getPath());
            }else if (file.isDirectory()){
                list(file.getPath().toString());
            }

        }
        fs.close();
    }

    /***
     * 文件从本地上传到
     * HDFS
     * @param src
     * @param dst
     * @return
     */
    public boolean uploadFromLocal(String src , String dst){
        try {
            fs.copyFromLocalFile(false,true, new Path(src), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("文件上传到HDFS出错");
            return false;
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("上传文件成功");
        return true;
    }

    /**
     * 文件下载
     * @param src
     * @param dst
     * @return
     */
    public boolean getFromHDFS(String src , String dst){
        Path dstPath = new Path(dst) ;
        try{
            fs.copyToLocalFile(false, new Path(src), dstPath) ;
        }catch(IOException ie){
            ie.printStackTrace();
            System.out.println("文件下载出错");
            return false ;
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("上传下载成功");
        return true ;
    }

    public void getNodeInfo() throws IOException{
        DistributedFileSystem hdfs = (DistributedFileSystem)fs;
        DatanodeInfo[] datanodeInfos = hdfs.getDataNodeStats();
        for (DatanodeInfo info: datanodeInfos) {
            System.out.println(info.getDatanodeReport());
        }
    }
}
