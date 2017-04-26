package DTree.datatype;

import java.util.HashMap;


public class Tree {
    int id = -1;//存储下一层属性号,叶子节点为-1
    String name;//属性名
    boolean leaf = false;//是否为叶子节点
    String label;//叶子节点的标签
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
                return "error";
            }
        }
    }
}
