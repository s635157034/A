package AdaBoost;

import AdaboostVerify.AdaboostVerifyMR;
import DTree.program.DecisionTreeDriver;
import DecesionStump.DecesionStump;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;


public class ABmain {
    public static long N;//抽取样本数量
    public static long T;//全部数据数量
    public static long max = 1;
    public static double MaxError = 1;
    public static ArrayList<ClassiferInfo> info = new ArrayList<ClassiferInfo>();
    public static ArrayList<ClassiferInfo> allinfo =new ArrayList<>();

    public static void main(String[] args) throws Exception {
        int i = 0;
        long startTime=System.currentTimeMillis();


        String root ="Adaboost";
        String data = root+"/data";//数据集
        String tempdata = root+"/tempdata";//包含权重的数据
        String sample = root+"/sample";//抽样文件
        String classiferPath =root+"/classifer";//分类器所在文件夹
        String ClassiferTmp=root+"/classifertmp";
        String verifyTmp =root+"/verifyTmp";//MR临时文件夹
        String total=root+"/total";//数据集总量
        String error=root+"/error";
        String totalWeight=root+"/totalWeight";
        String updateTmp=root+"/updateTmp";


        InitializeMR.run(data, tempdata, total);//初始化数据集(加入权重）
        T = ReadTotalnumber(total);
        N=T/3;

        do {
            i++;
            System.out.println("第"+i+"次抽样");

            String str = "-" + Integer.toString(i);
            Sampling(tempdata,sample+str,str);

            System.out.println("第"+i+"次进行分类");

            BuildClassifier(sample+str,classiferPath+"/classifer-"+i,ClassiferTmp+str);

            System.out.println("第"+i+"次分类结束");

            Verify(tempdata, verifyTmp + str, classiferPath + "/classifer" + str);

            if (!UpdateWeight(verifyTmp + str, updateTmp, error, total, totalWeight, tempdata, str))//错误率高于0.5则重新生成
            {
                //max++;
            }
            printInfo(root+ "/classiferInfo",info);
        } while (i < max);
        printInfo(root+"/allInfo",allinfo);
        AdaboostVerifyMR.run(root);
        long endTime=System.currentTimeMillis(); //获取结束时间
        WriteString(root+"/time","程序运行时间： "+formatDuring(endTime-startTime)+"s");
    }


    /*
     *从数据集中抽取数据
     */
    private static void Sampling(String data, String sample,String i) throws Exception {
        SamplingMR.run(data, sample, String.valueOf(N),i);
    }

    /*
     *根目录存储names
     *
     */
    private static void BuildClassifier(String data, String classiferPath, String DTtemp) throws Exception {
        //DecesionStump.DSrun(data,DTtemp,classiferPath);


        DecisionTreeDriver dt=new DecisionTreeDriver();
        dt.DTmain(data, DTtemp, classiferPath);
        //DTree.program.DecisionTreeDriver.DTmain(data, DTtemp, classiferPath);
        //DeleteFiles(DTtemp);//删除决策树产生的临时文件。
    }

    /*
     *根据分类器对检验集进行分类
     */
    private static void Verify(String data, String outputPath, String classifer) throws Exception {
        //MR从data读取数据写入到tmp中
        DTree.verify.VerifyMR.run(data, outputPath, classifer);
        DeleteFiles(data);
    }

    /*
     *计算错误率,根据错误率更新权值,将更新后的权值复制到tempdata中
     */
    private static boolean UpdateWeight(String verifyPath, String tmpPath, String errorPath, String total, String totalWeight, String tempdata, String i) throws Exception {
        UpdateWeightMR.run(verifyPath, tmpPath, errorPath, total, totalWeight,i);
        double errors=Double.valueOf(ReadString(errorPath));
        if(errors<MaxError)
        {
            //从tmpPath中读取数据，对数据集进行规范化
            FormatWeightMR.run(tmpPath,tempdata,totalWeight,i);
            DeleteFiles(tmpPath);
            info.add(new ClassiferInfo("classifer"+i,errors));//添加到info中
            allinfo.add(new ClassiferInfo("classifer"+i,errors));
            return true;
        }
        else
        {
            FileSystem fs=FileSystem.get(new Configuration());
            fs.rename(new Path(tmpPath),new Path(tempdata));
            allinfo.add(new ClassiferInfo("classifer"+i,errors));
            return false;
        }
    }


    public static void DeleteFiles(String paths) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(paths);
        FileSystem fs = path.getFileSystem(conf);
        fs.delete(path, true);
    }

    public static long ReadTotalnumber(String paths) throws IOException {
        return Long.valueOf(ReadString(paths));
    }

    public static void printInfo(String paths,ArrayList<ClassiferInfo> info) throws IOException {
        Iterator<ClassiferInfo> iterator = info.iterator();
        Configuration conf = new Configuration();
        Path path = new Path(paths);
        FileSystem fs = path.getFileSystem(conf);
        FSDataOutputStream fsDataOutputStream = fs.create(path,true);
        PrintWriter printWriter = new PrintWriter(fsDataOutputStream);
        while (iterator.hasNext()) {
            printWriter.println(iterator.next().toString());
        }
        printWriter.close();
        fsDataOutputStream.close();
    }

    public static void WriteString(String paths,String str) throws IOException {
        Configuration conf=new Configuration();
        Path path=new Path(paths);
        FileSystem fs = FileSystem.get(conf);
        PrintWriter pw=new PrintWriter(fs.create(path,true));
        pw.println(str);
        pw.close();
    }


    public static String ReadString(String paths) throws  IOException{
        Configuration conf=new Configuration();
        Path path=new Path(paths);
        FileSystem fs = FileSystem.get(conf);
        Scanner scanner=new Scanner(fs.open(path));
        String tmp=scanner.next();
        scanner.close();
        return tmp;
    }

    public static String formatDuring(long mss) {
        long days = mss / (1000 * 60 * 60 * 24);
        long hours = (mss % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
        long minutes = (mss % (1000 * 60 * 60)) / (1000 * 60);
        long seconds = (mss % (1000 * 60)) / 1000;
        return days + " 天 " + hours + " 小时 " + minutes + " 分钟 "
                + seconds + " 秒 ";
    }


}


