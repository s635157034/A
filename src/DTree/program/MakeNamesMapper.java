package DTree.program;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by root on 16-12-27.
 */
public class MakeNamesMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer str = new StringTokenizer(value.toString(),",");
        int k=0;
        while (str.hasMoreTokens())
        {
            context.write(new IntWritable(k),new Text(str.nextToken()));
            k++;
        }
    }
}
