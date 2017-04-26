package AdaBoosting;

import org.apache.commons.math3.optim.nonlinear.vector.Weight;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Scanner;


public class InitializeMR {

    public static void run(String data,String tempdata,String total) throws Exception {
        Configuration conf = new Configuration();
        conf.set("total",total);
        Job job = Job.getInstance(conf);
        job.setJobName("InitializeMR-1");
        job.setJarByClass(InitializeMR.class);
        job.setMapperClass(Initializemapper.class);
        job.setReducerClass(Initializereducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(tempdata));
        job.waitForCompletion(true);
        ABmain.DeleteFiles(tempdata);//删除空文件
        InitializeMRs.run(data,tempdata,conf);
    }




    static class Initializemapper extends Mapper<Object, Text, LongWritable, LongWritable> {
        long total=0;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            total++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(1),new LongWritable(total));
        }
    }

    static class Initializereducer extends  Reducer<LongWritable,LongWritable,Text,Text>{
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total=0;
            Iterator<LongWritable> it = values.iterator();
            while (it.hasNext())
            {
                total+=it.next().get();
            }
            Configuration conf=context.getConfiguration();
            ABmain.WriteString(conf.get("total"),String.valueOf(total));
        }
    }

}
class InitializeMRs{
    public static void run(String data,String tempdata,Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        conf.set("mapreduce.output.textoutputformat.separator", "#");
        Job job = Job.getInstance(conf);
        job.setJobName("InitializeMR-2");
        job.setJarByClass(InitializeMRs.class);
        job.setMapperClass(Initializesmapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(tempdata));
        job.waitForCompletion(true);

    }
    static class Initializesmapper extends Mapper<Object, Text, Text, Text> {
        Double weight=0.0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf=context.getConfiguration();
            long total=Long.valueOf(ABmain.ReadString(conf.get("total")));
            weight=1.0/total;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           context.write(value,new Text(weight.toString()));
        }
    }


}
