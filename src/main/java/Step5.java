import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class Step5 {
    /**
     * Input:
     * 1) key = lineId (LongWritable) value = decade w1 w2 \t npmi
     * 2) key = lineId (LongWritable) value = decade \t sum (sum of this npmi for this decade)
     * Output:
     * 1) key = <decade npmi> value = <w1 w2>
     * 2) key = <decade Integer.MAX_VALUE> value = <sum>
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t");
            String[] keyVals = fields[0].split(" ");
            String value = fields[1];

            //case 1 just decade in the key
            if (keyVals.length == 1){
                context.write(new Text(keyVals[0] + " " + Integer.MAX_VALUE), new Text(value));
            }
            //case 2 decade w1 w2 and npmi
            else{
                String decade = keyVals[0];
                String w1 = keyVals[1];
                String w2 = keyVals[2];
                String npmi = value;
                context.write(new Text(decade + " "+ npmi),new Text(w1 + " " + w2 ));
            }
        }
    }

    /**
     * Input:
     * 1) key = <decade npmi> value = <w1 w2>
     * 2) key = <decade Integer.MAX_VALUE> value = <sum>
     * Output:
     * 1) key = <decade w1 w2> value = npmi (if it is a collations)
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        protected double sum = 0;
        protected double minPmi;
        protected double relMinPmi;

        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            minPmi = Double.parseDouble(conf.get("minPmi"));
            relMinPmi = Double.parseDouble(conf.get("relMinPmi"));
        }

        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String [] keySplit = key.toString().split(" ");
            if (!keySplit[1].contains(".")){
                for (Text value:Values){
                    sum = Double.parseDouble(value.toString());
                }
            }
            else {
                double npmi = Double.parseDouble(keySplit[1]);
                if (npmi >= 0.96) return;
                String decade = keySplit[0];
                for(Text value:Values){
                     String [] vals = value.toString().split(" ");
                     String w1 = vals[0];
                     String w2 = vals[1];

                     //calculate condition 1
                    if (npmi >= minPmi){
                        context.write(new Text(decade + " " + npmi), new Text(w1 + " " + w2));
                    }
                    else if (npmi/sum >= relMinPmi){
                        context.write(new Text(decade + " " + npmi), new Text(w1 + " " + w2));
                    }
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
    private static class Comparison extends WritableComparator {
        protected Comparison() {
            super(Text.class, true);
        }
        //key = <decade npmi> or key = <decade Integer.MAX_VALUE>
        public int compare(WritableComparable key1, WritableComparable key2) {
            String[] words1 = key1.toString().split(" ");
            String[] words2 = key2.toString().split(" ");

            // Compare decades
            int decade1 = Integer.parseInt(words1[0]);
            int decade2 = Integer.parseInt(words2[0]);
            if (decade1 != decade2) {
                return Integer.compare(decade2, decade1); // Sort by decade in descending order
            }

            // Compare npmi values
            double npmi1 = Double.parseDouble(words1[1]);
            double npmi2 = Double.parseDouble(words2[1]);
            return Double.compare(npmi2, npmi1); // Sort by npmi in descending order
        }

    }
    public static void main(String[] args) throws Exception{
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        String minPmi = args[1];
        String relMinPmi = args[2];
        Configuration conf = new Configuration();
        conf.setDouble("minPmi", Double.parseDouble(minPmi));
        conf.setDouble("relMinPmi", Double.parseDouble(relMinPmi));
        Job job = Job.getInstance(conf, "Step5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(Step5.MapperClass.class);
        job.setSortComparatorClass(Step5.Comparison.class);
        job.setReducerClass(Step5.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step5.PartitionerClass.class);
//        conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes, more mappers
        FileInputFormat.addInputPath(job, new Path("s3://oriori316283464/output_step_4"));
        FileOutputFormat.setOutputPath(job, new Path("s3://oriori316283464/output_step_5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
