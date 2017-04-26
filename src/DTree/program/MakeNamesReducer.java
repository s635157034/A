package DTree.program;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MakeNamesReducer extends Reducer<IntWritable,Text,Text,Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> value = values.iterator();
        List<String> names =new ArrayList<>();
        String out=null;
        String name="属性"+key.toString();//默认用key当名字
        while (value.hasNext())
        {
            String tmp = value.next().toString();
            if(!names.contains(tmp))
            {
                names.add(tmp);
            }
        }
        out=name+":";
        for(String tmp : names)
        {
            out+=tmp+",";
        }
        out=out.substring(0,out.length()-1);
        context.write(new Text(out),null);
    }
}
