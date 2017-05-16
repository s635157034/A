package DecesionStump;

import AdaboostVerify.ErrorSumMR;
import DTree.datatype.Rule;
import DataFormat.DataFormat;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Created by root on 17-5-16.
 */
public class DecesionStump {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        DSrun();
    }

    public static void DSrun() throws InterruptedException, IOException, ClassNotFoundException {
        String root="Adaboost";
        String inputPath=root+"/data";
        String outputPath=root+"/Stump-temp";
        String outPath=root+"/Stump-classier";
        DSmapereduce.run(inputPath,outputPath,0);
        writeModelToFile(outputPath,outPath,0);
    }


    public static void writeModelToFile(String filePath,String outPath,int number) throws IOException {
        String maxTag=null,currentAttribute = null;
        String[] tmp=null;
        long max=0;
        String attribute = null;
        String tag = null;
        long times;
        Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(filePath));
        Path[] pathes = FileUtil.stat2Paths(files);
        PrintWriter pw = new PrintWriter(fs.create(new Path(outPath)));
        for (Path path : pathes) {
            // 跳过HDFS上的统计文件，这些文件一般以下划线开头
            if (path.getName().startsWith("_"))
                continue;
            Scanner scanner = new Scanner(fs.open(path));
            while (scanner.hasNext()) {
                tmp = scanner.next().split(",");
                attribute = tmp[0];
                tag = tmp[1];
                times = Long.valueOf(tmp[2]);
                if(currentAttribute==null){
                    currentAttribute=attribute;
                    maxTag=tag;
                    max=times;
                }
                else if (attribute.equals(currentAttribute)) {
                    if (times > max) {
                        max = times;
                        maxTag = tag;
                    }
                } else {
                    pw.println(String.valueOf(number) + "," + currentAttribute + ":" + maxTag);
                    max = times;
                    maxTag = tag;
                    currentAttribute = attribute;
                }
            }
            scanner.close();
        }
        pw.println(String.valueOf(number)+","+attribute+":"+maxTag);
        pw.close();
    }

}


class DSmapereduce{
    static public void run(String inputPath,String outputPath,int number) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.setInt("AttributeNumber",number);
        Job job = Job.getInstance(conf);
        job.setJobName("Decestion-Stump");
        job.setJarByClass(DSmapereduce.class);
        job.setMapperClass(DSMapper.class);
        job.setReducerClass(DSReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);


        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        job.waitForCompletion(true);

    }

    static class DSMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        int number;
        IntWritable one=new IntWritable(1);
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            number=context.getConfiguration().getInt("AttributeNumber",0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp=value.toString().split(",");
            String attribute=tmp[number];
            String tag=tmp[tmp.length-1];
            context.write(new Text(attribute+","+tag),one);
        }
    }

    static class DSReducer extends Reducer<Text,IntWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            Iterator<IntWritable> it=values.iterator();
            while (it.hasNext()){
                sum+=it.next().get();
            }
            context.write(key,new LongWritable(sum));
        }
    }

}