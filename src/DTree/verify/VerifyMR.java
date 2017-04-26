package DTree.verify;

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
public class VerifyMR {
    public static void run(String data,String out,String rules) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        /*String data="test-data/data";
        String tmp="verifyTEMP";
        String rules="test-data/model";*/
        conf.set("rules",rules);
        conf.set("mapreduce.output.textoutputformat.separator", "#");
        Job job=Job.getInstance(conf);
        job.setJobName("VerifyMR");
        job.setJarByClass(VerifyMR.class);
        job.setMapperClass(VerifyMapper.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(out));

        job.waitForCompletion(true);
    }


}
