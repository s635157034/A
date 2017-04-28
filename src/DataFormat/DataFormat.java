package DataFormat;

import AdaboostVerify.ErrorSumMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by root on 17-4-21.
 */
public class DataFormat {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        run("/adult");
    }

    public static void run(String root) throws IOException, ClassNotFoundException, InterruptedException {
        String data=root+"/data";
        String output=root+"/formatdata";
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "");

        Job job = Job.getInstance(conf);
        job.setJobName("Errorsum");
        job.setJarByClass(ErrorSumMR.class);
        job.setMapperClass(FormatMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);



    }


    static class FormatMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        Counter counter;
        Text text=new Text("error");
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            counter=context.getCounter("Error","含有？数据项");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(! (value.toString().split(",").length==15))
                context.write(value,text);

        }
    }
}


