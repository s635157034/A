package DTree.program;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Created by root on 16-12-27.
 */
public class MakeNames {
    public static void run(String data, String path, String tmp) throws Exception {
        tmp += "/namesMR";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MakeNames.class);
        job.setMapperClass(MakeNamesMapper.class);
        job.setReducerClass(MakeNamesReducer.class);


        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(data));
        FileOutputFormat.setOutputPath(job, new Path(tmp));

        job.waitForCompletion(true);


        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(tmp));
        Path[] pathes = FileUtil.stat2Paths(files);
        for (Path temp : pathes) {
            // 跳过HDFS上的统计文件，这些文件一般以下划线开头
            if (temp.getName().startsWith("_"))
                continue;
            FileUtil.copy(fs, temp, fs, new Path(path), false, conf);
        }
    }
}