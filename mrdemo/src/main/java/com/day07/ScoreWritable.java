package com.day07;

import org.apache.hadoop.io.WritableComparable;

import java.io.*;
public class ScoreWritable implements WritableComparable<ScoreWritable> {
    int chinese;
    int math;
    int english;
    int sum;

    public ScoreWritable() {
    }

    public ScoreWritable(int chinese, int math, int english) {
        this.chinese = chinese;
        this.math = math;
        this.english = english;
        this.sum=chinese+english+math;
    }

    @Override
    public String toString() {
        return "ScoreWritable{" +
                "chinese=" + chinese +
                ", math=" + math +
                ", english=" + english +
                ", sum=" + sum +
                '}';
    }

    public int getChinese() {
        return chinese;
    }

    public void setChinese(int chinese) {
        this.chinese = chinese;
    }

    public int getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }

    public int getEnglish() {
        return english;
    }

    public void setEnglish(int english) {
        this.english = english;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
    //比较
    public int compareTo(ScoreWritable that) {
        //先比较总成绩
        if (this.sum>that.getSum()){
            return -1;
        }else if(this.sum<that.getSum()){
            return 1;
        }else{
            if (this.chinese>that.getChinese()){
                return -1;
            }else if (this.chinese<that.getChinese()){
                return 1;
            }else {
                return -(this.math-that.getMath());
            }
        }
    }
    //序列化--dataOutput(data流)：可以自定义序列化对象，节省空间，hadoop用的就是这个流
    public void write(DataOutput out) throws IOException {
        out.writeInt(chinese);
        out.writeInt(math);
        out.writeInt(english);
        out.writeInt(sum);
    }
    //反序列化
    public void readFields(DataInput in) throws IOException {
        this.chinese = in.readInt();
        this.math = in.readInt();
        this.english = in.readInt();
        this.sum = in.readInt();
    }

    public static void main(String[] args) throws IOException {
        /*DataOutputStream out = new DataOutputStream(new FileOutputStream("d:\\test\\score"));
        ScoreWritable score = new ScoreWritable(1, 1, 100);
        score.write(out);
        out.close();*/

        /*DataInputStream in = new DataInputStream(new FileInputStream("d:\\test\\score"));
        ScoreWritable score2= new ScoreWritable();
        score2.readFields(in);
        System.out.println(score2);*/


    }
}
