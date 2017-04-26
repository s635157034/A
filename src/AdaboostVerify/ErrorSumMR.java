package AdaboostVerify;

import AdaBoosting.ABmain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.util.Iterator;

/**
 * Created by root on 17-4-16.
 */
public class ErrorSumMR {
    public static void main(String[] args) throws Exception {
        String root="adaboosting";
        String data=root+"/VerifyResults";
        String output=root+"/afdfds";
        String error=root+"/AdaboostErrors";
        String totalnum=root+"/total";
        run(data,output,error,totalnum);
    }



    public static void run(String data,String tmp,String error,String totalnum) throws Exception {
        Configuration conf = new Configuration();
        conf.set("error",error);
        conf.set("total",totalnum);
        conf.set("mapred.textoutputformat.separator", "#");

        Job job = Job.getInstance(conf);
        job.setJobName("Errorsum");
        job.setJarByClass(ErrorSumMR.class);
        job.setMapperClass(ErrorSummapper.class);
        job.setReducerClass(ErrorSumreducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(tmp));

        job.waitForCompletion(true);
        ABmain.DeleteFiles(tmp);//删除空文件
    }


    static class ErrorSummapper extends Mapper<LongWritable, Text,LongWritable,LongWritable> {
        long error=0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tmp=value.toString().substring(value.toString().lastIndexOf(',')+1);
            String result[]=tmp.split("#");
            if(!result[0].equals(result[1]))//被错误分类的情况
            {
                //统计error（Mi)
                error++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(1),new LongWritable(error));
        }
    }


    static class ErrorSumreducer extends Reducer<LongWritable,LongWritable,Text,Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long errors=0;
            Iterator<LongWritable> it = values.iterator();
            while (it.hasNext())
            {
                errors+=it.next().get();
            }
            Configuration conf=context.getConfiguration();
            long total= Long.valueOf(ABmain.ReadString(conf.get("total")));
            double error=(double) errors/(double) total;
            ABmain.WriteString(conf.get("error"),String.valueOf(error));
            System.out.println(error);
        }
    }


























}
