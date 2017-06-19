package Knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

/**
 * Created by root on 17-6-20.
 */
public class ABKnn {
    public static void main(String[] args) throws IOException {
        String data="KNN/wine-test";
        String result="KNN/wine-out"+"/part-r-00000";
        String output = "KNN/verify";
        append(new Path(data),new Path(result),new Path(output));
    }

    public static void append(Path data, Path result, Path output) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Scanner sdata = new Scanner(fs.open(data));
        Scanner sresult = new Scanner(fs.open(result));
        PrintWriter poutput=new PrintWriter(fs.create(output));
        while (sdata.hasNext() || sresult.hasNext()){
            poutput.println(sdata.next()+"#"+sresult.next().split("#")[1]);
        }
        sdata.close();
        sresult.close();
        poutput.close();
    }

}
