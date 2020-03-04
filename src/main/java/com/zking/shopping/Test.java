package com.zking.shopping;

public class Test {
    
    public static void main(String[] args) {
        String s = "'004148570',\n" +
                "\t\t'许金肥\\',null,null,null,' 10903030\\62005023371 ',null,null,null,' 004148566 \\',' 许金肥 '";
        String s2 = s.replaceAll("\\\\'", "\\\\\\\\'");
        String s3 = s.replaceAll("\\'", "\\\'");
        System.out.println(s);
        System.out.println(s2);
        System.out.println(s3);
    }
}
