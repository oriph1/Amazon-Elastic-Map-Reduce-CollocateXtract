import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class Step2 {

    /**
     * Input:
     * 1) key = lineId (LongWritable) value = decade \t occurrences
     * 2) key = lineId (LongWritable) value = decade w1 w2 \t occurrences
     * Output:
     * 1) key = <decade> value = <occurences>
     * 2) key = <decade w1 *> value = <occurences>
     * 3) key = <decade w1 **> value = <w2 occurences>
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t");
            String newKey = fields[0];
            String occurrences = fields[1];
            //only decade in the key
            if (newKey.split(" ").length == 1){
                context.write(new Text(newKey), new Text(occurrences));
            }
            else{
                String[] values = newKey.split(" ");
                String decade = values[0];
                String w1 = values[1];
                String w2 = values[2];
                context.write(new Text(decade + " " + w1 + " " + "*"),new Text(occurrences));
                context.write(new Text(decade + " " + w1 + " " + "**"),new Text(w2 + " " +occurrences));
            }
        }
    }

    /**
     * Input:
     * 1) key = <decade> value = <occurences>
     * 2) key = <decade w1 *> value = <occurences>
     * 3) key = <decade w1 **> value = <w2 occurences>
     * Output:
     * 1) key = <decade w1 w2> value = <occurrences c(w1)>
     * 2) key = <decade> value = <occurrences>
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        protected long cw1 = 0;

        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String [] keySplit = key.toString().split(" ");
            //case 1
            if (keySplit.length == 1){
                for (Text value: Values){
                    context.write(key,value);
                }
            }
            //case 2 *
            else if (keySplit[2].equals("*")){
                    for (Text value: Values){
                        long occurrence = Long.parseLong(value.toString());
                        cw1+=occurrence;
                    }
                }
                //Case 3 **
                else{
                    for (Text value: Values){
                        String decade = keySplit[0];
                        String w1 = keySplit[1];
                        String [] valueValues = value.toString().split(" ");
                        String w2 = valueValues[0];
                        String w1w2occurrence = valueValues [1];
                        context.write(new Text(decade +" "+ w1 + " " + w2),new Text(w1w2occurrence + " "
                        + cw1) );
                    }
                    cw1 = 0;
                }
            }
        }

    /**
     * Input:
     * 1) key = <decade> value = <occurences>
     * 2) key = <decade w1 *> value = <occurences>
     * 3) key = <decade w1 **> value = <w2 occurences>
     * Output:
     * 1) key = <decade> value = <occurences>
     * 2) key = <decade w1 *> value = <occurences>
     * 3) key = <decade w1 **> value = <w2 occurences>
     */
    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        protected long cw1= 0;
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String[] keySplit = key.toString().split(" ");
            if(keySplit.length >1 && keySplit[2].equals("*")){
                for (Text value : Values) {
                    long occurrence = Long.parseLong(value.toString());
                    cw1 += occurrence;
                }
                context.write(key, new Text(""+cw1));
                cw1 = 0;
            }
            else {
                for (Text value: Values){
                    context.write(key,value);
                }
            }
        }
    }

    //Partition by the decade
    public static class PartitionerClass extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().split(" ")[0].hashCode() % numPartitions);
        }
    }
    public static void main(String[] args) throws Exception{
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(Step2.MapperClass.class);
        job.setReducerClass(Step2.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step2.PartitionerClass.class);
        job.setCombinerClass(Step2.CombinerClass.class);
//        conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes, more mappers
        FileInputFormat.addInputPath(job, new Path("s3://oriori316283464/output_step_1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://oriori316283464/output_step_2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
