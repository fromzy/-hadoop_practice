package cn.zy.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


/**
 * map阶段的业务逻辑就写在自定义的map()方法中
 * maptask会对每一行输入数据调用一次我们自定义的map（）方法
 */

/**Mapper 类，有四个泛型，分别是KEYIN、VALUEIN、KEYOUT、VALUEOUT，
 * 前面两个KEYIN、VALUEIN 指的是map 函数输入的参数key、value 的类型；
 * 后面两个KEYOUT、VALUEOUT 指的是map 函数输出的key、value 的类型；
 * @param KEYIN →k1 表示每一行的起始位置（偏移量offset）
 * @param VALUEIN →v1 表示每一行的文本内容
 * @param KEYOUT →k2 表示每一行中的每个单词
 * @param VALUEOUT →v2 表示每一行中的每个单词的出现次数，固定值为1
 */

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    @Override //map函数接受文本，把结果以key-value的形式存储，
    // key-->LongWritable即文本的行号，value-->Text即被切割的每一行文本；
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        super.map(key, value, context);

        //1.获取一行数据

        String line =value.toString();

        //2.获取行中每一个单词,以 空格 切割
        String words[] = line.split(" ");
        for (String word : words){
            System.out.print(word);

            //3.输出单词
            //输出到maptask自己内部的缓冲区，在缓冲区中整理数据（分区，排序，合并...）,最后到reduceTask里；
            // 将单词作为key，将次数1作为value,以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reducetask中
            context.write(new Text(word),new IntWritable(1));//key-->Text是每个单词,value-->IntWritable是单词个数，
        }
    }
}
