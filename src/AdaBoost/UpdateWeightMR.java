package AdaBoost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.Iterator;


public class UpdateWeightMR {


    /*
    * 第一次mr统计错误率，第二次开始更新权重
    */
    public static void run(String data, String tmp, String error, String totalnum, String totalWeight, String i) throws Exception {
        Configuration conf = new Configuration();
        conf.set("error", error);
        conf.setDouble("MaxError",ABmain.MaxError);
        conf.set("total", totalnum);
        conf.set("totalweight", totalWeight);
        conf.set("mapred.textoutputformat.separator", "#");

        Job job = Job.getInstance(conf);
        job.setJobName("UpdateWeight" + i);
        job.setJarByClass(UpdateWeightMR.class);
        job.setMapperClass(UpdateWeightmapper.class);
        job.setReducerClass(UpdateWeightreducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(data));
        FileOutputFormat.setOutputPath(job, new Path(tmp));

        job.waitForCompletion(true);
        ABmain.DeleteFiles(tmp);//删除空文件
        UpdateWeightMRs.run(data, tmp, conf, i);
        UpdateWeightMRss.run(tmp, tmp, conf, i);
    }


    static class UpdateWeightmapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        long error = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp = value.toString().split(",");
            String[] result = tmp[tmp.length - 1].split("#");
            String[] str = value.toString().split("#");
            if (!result[0].equals(result[2]))//被错误分类的情况
            {
                //统计error（Mi)
                error++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(1), new LongWritable(error));
        }
    }


    static class UpdateWeightreducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long errors = 0;
            Iterator<LongWritable> it = values.iterator();
            while (it.hasNext()) {
                errors += it.next().get();
            }
            Configuration conf = context.getConfiguration();
            long total = Long.valueOf(ABmain.ReadString(conf.get("total")));
            double error = (double) errors / (double) total;
            ABmain.WriteString(conf.get("error"),String.valueOf(error));
        }
    }


}

class UpdateWeightMRs {

    public static void run(String data, String tmp, Configuration conf, String i) throws Exception {
        conf.set("mapred.textoutputformat.separator", "");
        Job job = Job.getInstance(conf);
        job.setJobName("UpdateWeightMRs" + i);
        job.setJarByClass(UpdateWeightMR.class);
        job.setMapperClass(UpdateWeightmapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(data));
        FileOutputFormat.setOutputPath(job, new Path(tmp));

        job.waitForCompletion(true);


    }

    static class UpdateWeightmapper extends Mapper<LongWritable, Text, Text, Text> {
        double change;
        Text outs=new Text("");
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            double error = Double.valueOf(ABmain.ReadString(conf.get("error")));
            change = error / (1 - error);
            double MaxError=conf.getDouble("MaxError",0.5);
            if (error >= MaxError)//错误率超过MaxError则不更新权重
                change = 1;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String values = value.toString();
            String[] tmp = values.split(",");
            String[] result = tmp[tmp.length - 1].split("#");//0是正确结果，2是分类结果,1是权重

            if (result[0].equals(result[2]))//正确分类的情况
            {
                String out = values.substring(0, values.lastIndexOf('#'));//删掉验证结果
                context.write(new Text(out),outs);
            } else//被错误分类的情况
            {
                double weight = Double.valueOf(result[1]);
                String out = values.substring(0, values.indexOf('#')) + "#" + String.valueOf(weight * change);//修改权重，删除验证结果
                context.write(new Text(out),outs);
            }

        }
    }


}
class UpdateWeightMRss {

    public static void run(String data, String tmp, Configuration conf, String i) throws Exception {

        Job job = Job.getInstance(conf);
        job.setJobName("UpdateWeightMRss" + i);
        job.setJarByClass(UpdateWeightMR.class);
        job.setMapperClass(UpdateWeightmapper.class);
        job.setReducerClass(UpdateWeightreducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(data));
        FileOutputFormat.setOutputPath(job, new Path(tmp+"-tmp"));
        job.waitForCompletion(true);
        ABmain.DeleteFiles(tmp+"-tmp");//删除空文件
    }
    static class UpdateWeightmapper extends Mapper<LongWritable,Text,LongWritable,DoubleWritable>{
        double totalWeight;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString().split("#")[1];
            totalWeight += Double.valueOf(str);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(1),new DoubleWritable(totalWeight));
        }
    }

    static class UpdateWeightreducer extends Reducer<LongWritable,DoubleWritable,Text,Text>{
        @Override
        protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> it=values.iterator();
            double totalWeight=0;
            while (it.hasNext())
            {
                totalWeight+=it.next().get();
            }
            ABmain.WriteString(context.getConfiguration().get("totalweight"),String.valueOf(totalWeight));
        }
    }
}
