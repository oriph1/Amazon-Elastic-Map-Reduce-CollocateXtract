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

public class Step3 {
    /**
     * Input:
     * 1) key = lineId (LongWritable) value = decade \t occurrences
     * 2) key = lineId (LongWritable) value = value = decade w1 w2 \t occurrences c(w1)
     * Output:
     * 1) key = <decade> value = <occurrences>
     * 2) key = <decade w2 *> value = <occurrences>
     * 3) key = <decade w2 **> value = <w1 occurrences c(w1)>
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t");
            String newKey = fields[0];
            String occurrences_with_cw1 = fields[1];
            //only decade in the key
            if (newKey.split(" ").length == 1) {
                context.write(new Text(newKey), new Text(occurrences_with_cw1));
            } else {
                String[] values = newKey.split(" ");
                String[] occurrencescw1 = occurrences_with_cw1.split(" ");
                String decade = values[0];
                String w1 = values[1];
                String w2 = values[2];
                context.write(new Text(decade + " " + w2 + " " + "*"), new Text(occurrencescw1[0]));
                context.write(new Text(decade + " " + w2 + " " + "**"), new Text(w1 + " " + occurrencescw1[0] + " " + occurrencescw1[1]));
            }
        }
    }

    /**
     * Input:
     * 1) key = <decade> value = <occurrences>
     * 2) key = <decade w2 *> value = <occurrences>
     * 3) key = <decade w2 **> value = <w1 occurrences c(w1)>
     * Output:
     * 1) key = <decade w1 w2> value = <occurrences c(w1) c(w2)>
     * 2) key = <decade> value = <occurrences>
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        protected long cw2 = 0;

        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String[] keySplit = key.toString().split(" ");
            //case 1
            if (keySplit.length == 1) {
                for (Text value : Values) {
                    context.write(key, value);
                }
            }
            //case 2 and 3
            else if (keySplit[2].equals("*")) {
                for (Text value : Values) {
                    long occurrence = Long.parseLong(value.toString());
                    cw2 += occurrence;
                }
            }
            //Case 3
            else {
                //key = <decade w2 !!> value = <w1 occurrences c(w1)>
                for (Text value : Values) {
                    String decade = keySplit[0];
                    String w2 = keySplit[1];
                    String[] valueValues = value.toString().split(" ");
                    String w1 = valueValues[0];
                    String w1w2occurrence = valueValues[1];
                    String cw1 = valueValues[2];
                    context.write(new Text(decade + " " + w1 + " " + w2), new Text(w1w2occurrence + " "
                            + cw1 + " " + cw2));
                }
                cw2 = 0;
            }
        }
    }

    /**
     * Input:
     * 1) key = <decade> value = <occurrences>
     * 2) key = <decade w2 *> value = <occurrences>
     * 3) key = <decade w2 **> value = <w1 occurrences c(w1)>
     * Output:
     * 1) key = <decade> value = <occurrences>
     * 2) key = <decade w2 *> value = <occurrences>
     * 3) key = <decade w2 **> value = <w1 occurrences c(w1)>
     */
    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        protected long cw2 = 0;

        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String[] keySplit = key.toString().split(" ");
            if(keySplit.length >1 && keySplit[2].equals("*")){
                for (Text value : Values) {
                    long occurrence = Long.parseLong(value.toString());
                    cw2 += occurrence;
                }
                context.write(key, new Text(""+cw2));
                cw2 = 0;
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

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(Step3.MapperClass.class);
        job.setReducerClass(Step3.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step3.PartitionerClass.class);
        job.setCombinerClass(Step3.CombinerClass.class);
//        conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes, more mappers
        FileInputFormat.addInputPath(job, new Path("s3://oriori316283464/output_step_2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://oriori316283464/output_step_3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
