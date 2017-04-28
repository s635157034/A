package AdaboostVerify;

import DTree.datatype.Tree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by root on 17-4-14.
 */
public class AdaboostVerifyMapper extends Mapper<LongWritable,Text,Text,Text> {
    Decision decision=new Decision();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf=context.getConfiguration();
        String classiferPath=conf.get("rules");
        decision.classiferPath=classiferPath;
        String info=conf.get("info");
        FileSystem fs=FileSystem.get(conf);
        FSDataInputStream fsDataInputStream=fs.open(new Path(info));
        Scanner scanner=new Scanner(fsDataInputStream);
        while (scanner.hasNext())
        {
            String str=scanner.next();//添加分类器信息
            decision.add(str.split(":"));
        }
        scanner.close();
        fsDataInputStream.close();
        decision.addAttribute();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        Text text=new Text(decision.verify(value.toString()));
        Text text=new Text(decision.verifyInfo(value.toString()));
        context.write(value,text);
    }
}

class Decision{
    List<ClassiferInfo> classiferInfList=new ArrayList<>();
    String classiferPath;
    public void add(String info[]) throws IOException {
        String name=info[0];
        String error=info[1];
        ClassiferInfo tmp=new ClassiferInfo(name,Double.valueOf(error));
        addrules(name,tmp);
        classiferInfList.add(tmp);
    }
    private void addrules(String name,ClassiferInfo classiferInfo) throws IOException {
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);
        Scanner scanner=new Scanner(fs.open(new Path(classiferPath+"/"+name)));
        while(scanner.hasNext()){
            String str=scanner.next();
            classiferInfo.treeRoot.addRule(str);
        }
    }

    public String verify(String str){
        HashMap<String,Double> result=new HashMap<>();
        double maxWeight=-1000.0;
        String maxKey="error";
        String verifyTmp;
        for(ClassiferInfo tmp : classiferInfList){
            verifyTmp = tmp.treeRoot.verify(str);
            //使error的权重最低
            if(verifyTmp=="error")
            {
                result.put("error",-1000.0);
                continue;
            }
            if(result.containsKey(verifyTmp))
            {
                double weight=result.get(verifyTmp);
                weight+=tmp.weight;
                if(weight>maxWeight)
                {
                    maxKey=verifyTmp;
                }
                result.put(verifyTmp,weight);
            }
            else{
                result.put(verifyTmp,tmp.weight);
            }
        }
        return maxKey;
    }

    public String verifyInfo(String s){
        HashMap<String,Double> result=new HashMap<>();
        double maxWeight=0;
        String maxKey="error";
        String verifyTmp;
        double verifyWeight;
        for(ClassiferInfo tmp : classiferInfList){
            Tree.VerifyInfo verifyInfo=tmp.treeRoot.verifyInfo(s);
            verifyTmp=verifyInfo.label;
            verifyWeight=verifyInfo.weight;
            //verifyWeight= verifyInfo.weight == 1 ?1:0;
            if(result.containsKey(verifyTmp))
            {
                double weight=result.get(verifyTmp);
                weight+=tmp.weight*verifyWeight;
                if(weight>maxWeight)
                {
                    maxKey=verifyTmp;
                }
                result.put(verifyTmp,weight);
            }
            else{
                result.put(verifyTmp,tmp.weight*verifyWeight);
            }
        }
        return maxKey;
    }

    public void addAttribute(){
        for(ClassiferInfo tmp : classiferInfList){
            tmp.treeRoot.addAttribute();
        }

    }

}




class ClassiferInfo {
    public String name;
    public double error;
    public double weight;
    public Tree treeRoot;
    public ClassiferInfo(String name, double error) {
        this.name = name;
        this.error = error;
        weight=-1*Math.log(error/(1-error));
        treeRoot=new Tree(name);
    }

    @Override
    public String toString() {
        return name+":"+String.valueOf(error);
    }
}