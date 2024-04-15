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

public class Step4 {
    /**
     * Input:
     * 1) key = lineId (LongWritable) value = decade \t occurrences
     * 2) key = lineId (LongWritable) value = decade w1 w2 \t occurrences c(w1) c(w2)
     * Output:
     * 1) key = <decade *> value = <occurrences>  (N)
     * 2) key = <decade **> value = <w1 w2 occurrences c(w1) c(w2)>
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t");
            String newKey = fields[0];
            String occurrences = fields[1];
            //only decade in the key
            if (newKey.split(" ").length == 1){
                context.write(new Text(newKey + " " + "*"), new Text(occurrences));
            }
            else{
                String[] keyvalues = newKey.split(" ");
                String decade = keyvalues[0];
                String w1 = keyvalues[1];
                String w2 = keyvalues[2];
                String[] values = occurrences.split(" ");
                String occurr = values[0];
                String cw1 = values[1];
                String cw2 = values[2];
                context.write(new Text(decade + " "+ "**"),new Text(w1 + " " + w2 + " " + occurr + " "
                + cw1 + " " + cw2));
            }
        }
    }

    /**
     * Input:
     * 1) key = <decade *> value = <occurrences>  (N)
     * 2) key = <decade **> value = <w1 w2 occurrences c(w1) c(w2)>
     * Output:
     * 1) key = <decade w1 w2> value = npmi
     * 2) key = <decade> value = <sum> (sum of this npmi for this decade)
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        protected double sum = 0;
        protected double N = 0;

        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            String [] keySplit = key.toString().split(" ");
            if (keySplit[1].equals("*")){
                for (Text value: Values){
                    N = Double.parseDouble(value.toString());
                }
            }
            else {
                String decade = keySplit[0];
                for(Text value: Values) {
                    String [] valuesSplit =value.toString().split(" ");
                    String w1 = valuesSplit[0];
                    String w2 = valuesSplit[1];
                    double occurrences = Double.parseDouble(valuesSplit[2]);
                    double cw1 = Double.parseDouble(valuesSplit[3]);
                    double cw2 = Double.parseDouble(valuesSplit[4]);

                    //Calculate p(w1,w2)
                    double pw1w2 = occurrences / N;

                    //Calculate pmi
                    double pmiw1w2 = (Math.log(occurrences) + Math.log(N) - Math.log(cw1) - Math.log(cw2));

                    //Calculate npmi
                    double npmi = (pmiw1w2 / (-1 *(Math.log(pw1w2))));
                    if(!Double.isNaN(npmi)) {
                        sum += npmi;
                        context.write(new Text(decade + " " + w1 + " " + w2), new Text("" + npmi));
                    }
                }
                if (sum != 0) {
                    context.write(new Text(decade), new Text("" + sum));
                    sum = 0;
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
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(Step4.MapperClass.class);
        job.setReducerClass(Step4.ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(Step4.PartitionerClass.class);
//        conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes, more mappers
        FileInputFormat.addInputPath(job, new Path("s3://oriori316283464/output_step_3"));
        FileOutputFormat.setOutputPath(job, new Path("s3://oriori316283464/output_step_4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
