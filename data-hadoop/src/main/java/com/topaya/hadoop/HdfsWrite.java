package com.topaya.hadoop;

import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsWrite {
    public static void main(String[] args) {
        String hdfsPath = "hdfs://namenode";
        String filePath = "/testme.txt";
        String message = "Hola Hdfs!\n Test me";

        try {
            URI uri = new URI(hdfsPath);
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://namenode");
            conf.set("dfs.client.use.datanode.hostname", "true");
            conf.set("dfs.datanode.use.datanode.hostname", "true");
            // en caso deseas pasar muchas mas configuraciones se recomienda crear folder con
            // archivos xml de condifuracion
            // conf.addResource(new
            // Path("/Users/dsusanibar/Documents/ddsa/oss/data-evolutionary/infra/hadoop/java_home/core-site.xml"));
            // conf.addResource(new
            // Path("/Users/dsusanibar/Documents/ddsa/oss/data-evolutionary/infra/hadoop/java_home/hdfs-site.xml"));

            FileSystem fs = FileSystem.get(uri, conf);
            Path path = new Path(filePath);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            OutputStream os = fs.create(path);
            os.write(message.getBytes());
            os.flush();
            os.close();
            fs.close();

            System.out.println("El archivo ha sido creado exitosamente!");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
