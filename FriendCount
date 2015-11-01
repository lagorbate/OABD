import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendCount{

public static class TokenizerMapper
     extends Mapper<Object, Text, Text, IntWritable>{

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private int counter = 0;

public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
              String[] tokens = value.toString().split(";");
              if(counter==0)tokens[2]="0";
              int age = Integer.parseInt(tokens[2]);
              String tranche = new String();
              if(age<5)tranche="0-5";
              else if(age>5 && age<12)tranche="6-12";
              else if(age>12 && age<17)tranche="13-17";
              else if(age>17 && age<25)tranche="18-25";
              else if(age>25 && age<35)tranche="26-35";
              else if(age>35 && age<45)tranche="36-45";
              else if(age>45 && age<60)tranche="46-60";
              else if(age>60)tranche="60+";
              if(counter==0)tokens[4]="0";
              int friends = Integer.parseInt(tokens[4]);
              counter++;
              context.write(new Text(tranche), new IntWritable(friends));
       }
    }



public static class Reduce
  extends Reducer<Text, IntWritable, Text, IntWritable> {
       public void reduce(Text key, Iterable<IntWritable> values,
                          Context context
                          ) throws IOException,InterruptedException {
              int sum = 0;
              int counter = 0;
              for (IntWritable val : values) {
                   sum += val.get();
                   counter++;
              }
              int avrg = sum/counter;
       context.write(key , new IntWritable(avrg));
       }
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "Friend count");
  job.setJarByClass(FriendCount.class);
  job.setMapperClass(TokenizerMapper.class);
  job.setCombinerClass(Reduce.class);
  job.setReducerClass(Reduce.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
