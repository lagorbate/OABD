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

public class BankCount{

public static class TokenizerMapper
     extends Mapper<Object, Text, Text, IntWritable>{

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private counter = 0;

public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
              String[] tokens = value.toString().split(";");
              String location = tokens[1];
              if(counter==0)tokens[3]="0";
              int account = Integer.parseInt(tokens[3]);
              counter++;
              context.write(new Text(location), new IntWritable(account));
       }
    }



public static class Reduce
  extends Reducer<Text, IntWritable, Text, IntWritable> {
       public void reduce(Text key, Iterable<IntWritable> values,
                          Context context
                          ) throws IOException,InterruptedException {
              int sum = 0;
              for (IntWritable val : values) {
                   sum += val.get();
              }
       context.write(key , new IntWritable(sum));
       }
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "Bank count");
  job.setJarByClass(BankCount.class);
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
