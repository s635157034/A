package AdaboostVerify;

import AdaBoost.ABmain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by root on 17-4-14.
 */
public class AdaboostVerifyMR {
    public static void main(String[] args) throws Exception {
        String root="/root/桌面/test";
        run(root);
    }
    public static void run(String root) throws Exception {

        Configuration conf=new Configuration();
        String data=root+"/data";
        String testdata="/root/桌面/Adaboost数据/adult/testdata";
        String results=root+"/VerifyResults-all";
        String classiferPath=root+"/classifer";
        String classiferinfo=root+"/classiferInfo";

        ABmain.DeleteFiles(results);

        conf.set("rules",classiferPath);
        conf.set("info",classiferinfo);
        conf.set("mapreduce.output.textoutputformat.separator", "#");
        Job job=Job.getInstance(conf);
        job.setJobName("ABverify");


        job.setJarByClass(AdaboostVerifyMR.class);
        job.setMapperClass(AdaboostVerifyMapper.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(results));

        job.waitForCompletion(true);


        ErrorSumMR.run(results,root+"123",results+"/errors",root+"/total");
    }


}
