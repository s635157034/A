package KNN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import javax.ws.rs.core.NewCookie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 把预测的数据读入内存然后进行迭代计算 
 * 适用于预测数据很少训练数据很多 
 * 如果预测数据很多可以切分多分分别计算 
 * @author lenovo
 * 1,计算欧式距离(可根据实际情况修改距离公式) 
 * 2,找出最近 
 *   输出topk使用TreeSet<TopKeyWritable>自己写TopKeyWritable排序 
 */
public class Knn extends Configured implements Tool {

    public static enum Counter {
        PARSER_ERR
    }

    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
        private Text mykey = new Text();
        private Text myval = new Text();
        List  testList=new ArrayList();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            // 获得当前作业的DistributedCache相关文件
            Path path=new Path("KnnTest/TMP/data");
            Scanner scanner=new Scanner(FileSystem.get(context.getConfiguration()).open(path));
            while(scanner.hasNext()){
                testList.add(scanner.next());
            }
        }

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] array = value.toString().split(",");
            for (int i = 0; i < testList.size(); i++) {
                String[] c=testList.get(i).toString().split(",");
                Double[] re={0.0,0.0,0.0,0.0};
                for (int j = 0; j < c.length-1; j++) {
//                  System.out.println(c[j]+"xx"+array[j]+"xx"+(Double.parseDouble(c[j])-Double.parseDouble(array[j])));  
                    re[j]=Math.pow((Double.parseDouble(c[j])-Double.parseDouble(array[j])), 2);
                }
                Double plog=0.0;
                for (int j = 0; j < re.length; j++) {
                    plog+=re[j];
                }
                plog=Math.sqrt(plog);

                mykey.set(testList.get(i).toString());
                myval.set(value.toString()+"-"+plog);
                context.write(mykey, myval);
            }
        };
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        private Text val = new Text();
        Map top=new TreeMap();

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 循环遍历 Interable  
            for (Text value : values) {
                // 累加  
                String[] array = value.toString().split("-");
                top.put(array[1], array[0]);
            }
            String vaString=((TreeMap) top).lastEntry().getValue().toString();
            val.set(vaString+"-"+((TreeMap) top).lastEntry());
            context.write(key, val);
        };
    }

    @Override
    public int run(String[] args) throws Exception {
        // 1 conf  
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "-");// key value分隔符  
        DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);// 为该job添加缓存文件  
        // 2 create job  
        // Job job = new Job(conf, ModuleMapReduce.class.getSimpleName());  
        Job job = this.parseInputAndOutput(this, conf, args);
        // 3 set job  
        // 3.1 set run jar class  
        // job.setJarByClass(ModuleReducer.class);  
        // 3.2 set intputformat  
        job.setInputFormatClass(TextInputFormat.class);
        // 3.3 set input path  
        // FileInputFormat.addInputPath(job, new Path(args[0]));  
        // 3.4 set mapper  
        job.setMapperClass(MyMap.class);
        // 3.5 set map output key/value class  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 3.6 set partitioner class  
        // job.setPartitionerClass(HashPartitioner.class);  
        // 3.7 set reduce number  
//       job.setNumReduceTasks(0);  
        // 3.8 set sort comparator class  
        // job.setSortComparatorClass(LongWritable.Comparator.class);  
        // 3.9 set group comparator class  
        // job.setGroupingComparatorClass(LongWritable.Comparator.class);  
        // 3.10 set combiner class  
        // job.setCombinerClass(null);  
        // 3.11 set reducer class  
        job.setReducerClass(MyReduce.class);
        // 3.12 set output format  

        job.setOutputFormatClass(TextOutputFormat.class);
        // 3.13 job output key/value class  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 3.14 set job output path  
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        // 4 submit job  
        boolean isSuccess = job.waitForCompletion(true);
        // 5 exit  
        // System.exit(isSuccess ? 0 : 1);  
        return isSuccess ? 0 : 1;
    }

    public Job parseInputAndOutput(Tool tool, Configuration conf, String[] args)
            throws Exception {
        // validate  
//      if (args.length != 2) {  
//          System.err.printf("Usage:%s [genneric options]<input><output>\n",  
//                  tool.getClass().getSimpleName());  
//          ToolRunner.printGenericCommandUsage(System.err);  
//          return null;  
//      }  
        // 2 create job  
        Job job = new Job(conf, tool.getClass().getSimpleName());
        // 3.1 set run jar class  
        job.setJarByClass(tool.getClass());
        // 3.3 set input path  
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 3.14 set job output path  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job;
    }

    public static void main(String[] args) throws Exception {
        args = new String[] {
                "KnnTest/data","KnnTest/output","KnnTest/TMP/knntest.txt"};
        // run mapreduce
        int status = ToolRunner.run(new Knn(), args);
        // 5 exit  
        System.exit(status);
    }
} 