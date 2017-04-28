package DTree.datatype;

import java.util.ArrayList;
import java.util.HashMap;


public class Tree {
    int id = -1;//存储下一层属性号,叶子节点为-1
    String name;//属性名
    boolean leaf = false;//是否为叶子节点
    String label;//叶子节点的标签
    double weight=1;
    HashMap<String, Tree> tree = new HashMap<>();

    public Tree(String name) {
        this.name = name;
    }


    public void addRule(String s) {
        String str[] = s.split("&", 2);//0是第一个属性，1是后面全部
        String tmp[] = str[0].split(",");//0是id,1是属性名
        if (str.length == 1)//叶子节点
        {
            if(id==-1)
                this.id = Integer.valueOf(tmp[0]);
            String name = tmp[1].split(":")[0];
            String labels = tmp[1].split(":")[1];
            Tree temp = new Tree(name);
            temp.label = labels;
            temp.id = -1;
            temp.tree = null;
            temp.leaf = true;
            tree.put(name,temp);
        } else {
            boolean flag = tree.containsKey(tmp[1]);
            if (flag)//在已有的节点添加新的节点
            {
                tree.get(tmp[1]).addRule(str[1]);
            } else//创建新的节点
            {
                if (id == -1)
                    id = Integer.valueOf(tmp[0]);
                Tree temp = new Tree(tmp[1]);
                temp.addRule(str[1]);
                tree.put(tmp[1], temp);
            }
        }

    }


    public String verify(String s) {
        String str[] = s.split(",");
        if (leaf == true)
            return label;
        else {
            String names = str[id - 1];
            if (tree.containsKey(names)) {
                return tree.get(names).verify(s);
            } else {
                //return "error";
                if(label==null){
                    return "error";
                }
                else{
                    return label;
                }
            }
        }
    }

    public VerifyInfo verifyInfo(String s){
        String str[] = s.split(",");
        if (leaf == true)
            return new VerifyInfo(label);
        else {
            String names = str[id - 1];
            if (tree.containsKey(names)) {
                return tree.get(names).verifyInfo(s);
            } else {
                return new VerifyInfo(label,weight);
            }
        }
    }



    public void addAttribute(){
        if(!leaf)
        {
            HashMap<String,Counter> max=new HashMap<>();
            int maxcount,total=0;
            String name;
            for(Tree tmp : tree.values()){
                tmp.addAttribute();
                name=tmp.label;
                if(max.containsKey(name)){
                    max.get(name).count++;
                }
                else{
                    max.put(name,new Counter(name));
                }
            }
            maxcount=0;
            name=null;
            for(Counter tmp : max.values()){
                total+=tmp.count;
                if(tmp.count>maxcount){
                    name=tmp.label;
                    maxcount=tmp.count;
                }
            }
            this.label=name;
            this.weight=(double) maxcount/total;
        }

    }
    class Counter{
        String label;
        int count=0;

        public Counter(String label) {
            this.label = label;
            count++;
        }
    }
    public class VerifyInfo{
        public String label;
        public double weight=1;

        public VerifyInfo(String label) {
            this.label = label;
        }

        public VerifyInfo(String label, double weight) {
            this.label = label;
            this.weight = weight;
        }
    }
}
