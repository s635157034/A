package AdaBoost;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class FormatWeightMR {

    public static void run(String data,String tmp,String totalWeight,String i) throws Exception {
        Configuration conf = new Configuration();
        conf.set("totalWeight",totalWeight);
        conf.set("mapred.textoutputformat.separator", "#");

        Job job=Job.getInstance(conf);
        job.setJobName("formatweight"+i);
        job.setJarByClass(FormatWeightMR.class);
        job.setMapperClass(Formatmapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(data));
        FileOutputFormat.setOutputPath(job,new Path(tmp));

        job.waitForCompletion(true);
    }


    static class Formatmapper extends Mapper<Object,Text,Text,Text>
    {
        double format;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf=context.getConfiguration();
            double totalWeight=Double.valueOf(ABmain.ReadString(conf.get("totalWeight")));
            format=1/totalWeight;
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str[]=value.toString().split("#");
            double weight=Double.valueOf(str[1]);
            weight*=format;
            context.write(new Text(str[0]),new Text(String.valueOf(weight)));
        }
    }
}
