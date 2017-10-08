package hdfs;

import junit.framework.TestCase;

public class HDFSClientTest extends TestCase {


    HDFSClient hdfsClient;

    public void setUp() throws Exception {
        hdfsClient = new HDFSClient();
    }

    public void testGetFromHDFS() throws Exception {
        hdfsClient.getFromHDFS("/user/root/output/part-r-00000","out/result.txt");
    }

    public void testUploadFromLocal() throws Exception {
        hdfsClient.uploadFromLocal("input/1901","/user/root/input");
    }

    public void testDelete() throws Exception {
        hdfsClient.delete("/user/root/output", true);
    }

    public void testList() throws Exception {
        hdfsClient.list("/");
    }

    public void testGetNodeInfo() throws Exception {
        hdfsClient.getNodeInfo();
    }

}