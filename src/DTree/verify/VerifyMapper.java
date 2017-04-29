package DTree.verify;

import DTree.datatype.Tree;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Objects;
import java.util.Scanner;

/**
 * Created by root on 17-4-14.
 */
public class VerifyMapper extends Mapper<LongWritable,Text,Text,Text> {
    Tree treeRoot = new Tree(null);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String str=context.getConfiguration().get("rules");
        Path path=new Path(str);
        FileSystem fs=FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream=fs.open(path);
        Scanner scanner=new Scanner(fsDataInputStream);
        while (scanner.hasNext())
        {
            treeRoot.addRule(scanner.next());
        }
        scanner.close();
        fsDataInputStream.close();
        treeRoot.addAttribute();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Text text=new Text(treeRoot.verify(value.toString()));
        Text text=new Text(treeRoot.verifyInfo(value.toString()).label);
        context.write(value,text);
    }
}
