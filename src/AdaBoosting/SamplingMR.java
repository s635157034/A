package AdaBoosting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by root on 17-1-7.
 */
public class SamplingMR {
    public static void run(String data, String sample, String num,String i) throws Exception {
        Configuration conf = new Configuration();

        conf.set("number", num);

        Job job = Job.getInstance(conf);
        job.setJobName("samplingMR"+i);

        job.setJarByClass(SamplingMR.class);
        job.setMapperClass(Samplingmapper.class);
        job.setReducerClass(Samplingreducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(data));
        FileOutputFormat.setOutputPath(job, new Path(sample));

        job.waitForCompletion(true);
    }

    static class Samplingmapper extends Mapper<Object, Text, IntWritable, Text> {
        Random random = new Random();
        IntWritable a = new IntWritable(1);
        IntWritable b = new IntWritable(2);
        long num = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            num = new Long(context.getConfiguration().get("number"));
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("#");
            double weight = new Double(str[1]);
            //进行抽样
            if (random.nextDouble() <= weight * num) {
                context.write(a, new Text(str[0]));
            } else {
                if (random.nextDouble() <= weight * num) {
                    context.write(b, new Text(str[0]));
                }
            }
        }
    }

    static class Samplingreducer extends Reducer<IntWritable, Text, Text, Text> {
        int num = 0;
        int count = 0;

        //获取抽样数量
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            num = new Integer(context.getConfiguration().get("number"));
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> value = values.iterator();
            while (count < num && value.hasNext()) {
                //写入num个数据，数据少于num个则重复写入，否则加入到检验集中。
                Text text = value.next();
                context.write(text, null);
                count++;
            }
        }

    }
}

