package DecesionStump;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Created by root on 17-5-16.
 */
public class DecesionStump {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String root="Adaboost";
        String inputPath=root+"/data";
        String outputPath=root+"/Stump-temp";
        String outPath=root+"/Stump-classier-temp";
        String resultPath=root+"/Stump-classier";
        DSrun(inputPath,outPath,resultPath);
    }

    public static void DSrun(String datasetPath, String tempPath, String modelPath) throws InterruptedException, IOException, ClassNotFoundException {
        DSmapereduce.run(datasetPath,tempPath,1);
        writeModelToFile(tempPath,tempPath+"-tmp");
        chooseAttribute(tempPath+"-tmp",modelPath);
    }


    public static void writeModelToFile(String filePath,String outPath) throws IOException {
        String maxTag=null,currentAttribute = null,currentNumber=null;
        String[] tmp;
        String number,attribute,tag;
        long max=0;
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
                number=tmp[0];
                attribute = tmp[1];
                tag = tmp[2];
                times = Long.valueOf(tmp[3]);
                if(currentAttribute==null){
                    currentNumber=number;
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
                    pw.println(currentNumber + "," + currentAttribute + ":" + maxTag+"#"+max);
                    max = times;
                    maxTag = tag;
                    currentAttribute = attribute;
                    currentNumber = number;
                }
            }
            scanner.close();
        }
        pw.println(currentNumber + "," + currentAttribute + ":" + maxTag+"#"+max);
        pw.close();
    }
    public static void chooseAttribute(String filePath,String outPath) throws IOException {
        String currentNumber = null,maxNumber=null;
        long sum=0,maxsum=0;
        Configuration conf = new Configuration();
        FileSystem fs=FileSystem.get(conf);
        Scanner scanner = new Scanner(fs.open(new Path(filePath)));
        while (scanner.hasNext()) {
            String tmp=scanner.nextLine();
            if(currentNumber==null){
                currentNumber=tmp.split(",")[0];
                sum+=Long.valueOf(tmp.split("#")[1]);
            }
            else if(currentNumber.equals(tmp.split(",")[1])){
                sum+=Long.valueOf(tmp.split("#")[1]);
            }
            else{
                if(maxsum<sum){
                    maxsum=sum;
                    maxNumber=currentNumber;
                }
                sum=0;
            }
        }
        scanner.close();
        scanner=new Scanner(fs.open(new Path(filePath)));
        PrintWriter pw=new PrintWriter(fs.create(new Path(outPath)));
        while (scanner.hasNext()) {
            String tmp=scanner.next();
            if(maxNumber.equals(tmp.split(",")[0])){
                pw.println(tmp.split("#")[0]);
            }
        }
        pw.close();
        scanner.close();
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
        job.setCombinerClass(DSReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);


        FileInputFormat.setInputPaths(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        job.waitForCompletion(true);

    }

    static class DSMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        LongWritable one=new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp=value.toString().split(",");
            int len=tmp.length-1;
            String tag=tmp[len];
            for(int i=0;i<len-1;i++){
                String attribute=tmp[i];
                context.write(new Text(i+1+","+attribute+","+tag),one);
            }


        }
    }

    static class DSReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            Iterator<LongWritable> it=values.iterator();
            while (it.hasNext()){
                sum+=it.next().get();
            }
            context.write(key,new LongWritable(sum));
        }
    }

}