import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Step1 {
    //Step 1 :divide to decades (2000, 2010, 1980, .....) and count each 2 gram occurences
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> stopWords = new HashMap<>();
//        String letters = "^[a-zA-Z]+$";

        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            String[] Stop = {
                    "a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost",
                    "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst",
                    "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere",
                    "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes",
                    "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between",
                    "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant",
                    "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do",
                    "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
                    "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere",
                    "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for",
                    "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get",
                    "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
                    "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how",
                    "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into",
                    "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less",
                    "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more",
                    "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely",
                    "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor",
                    "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
                    "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out",
                    "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same",
                    "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show",
                    "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something",
                    "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that",
                    "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore",
                    "therein", "thereupon", "these", "they", "thick", "thin", "third", "this", "those", "though",
                    "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward",
                    "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
                    "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence",
                    "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which",
                    "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with",
                    "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"
            };
            String[] stopWordsArrHeb ={"של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה"
                    , "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
                    "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו",
                    "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", "אלה", "אל",
                    "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"","״","׳",
                    "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל", "יודע",
                    "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם", "אמרו", "אינם", "אחרי",
                    "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
                    "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן", "היתה", "הא", "ה", "בל", "בין",
                    "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"};

            for (String word : stopWordsArrHeb)
                stopWords.put(word, 0);

        }

        /**
         * Input:
         * Key = lineId (LongWritable)    Value = 2-gram \t year \t occurrences \t pages (Text)
         * Output:
         * 1) key = <decade> value = <occurrences>
         * 2) key = <decade w1 w2> value = <occurrences>
         */
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = line.toString().split("\t");
            if(fields.length > 1) {
                String[] twograms = fields[0].split(" ");
                if (twograms.length > 1) {
                    String w1 = twograms[0];
                    String w2 = twograms[1];

                    //Do english and hebrew for now just english
                    if (stopWords.containsKey(w1) || stopWords.containsKey(w2))
                        return;
//                    if (!w1.matches(letters) || !w2.matches(letters))
//                        return;
                    String year = fields[1];
                    String decade = year.substring(0, year.length()-1) + "0";
                    Text occurrences = new Text(fields[2]);
                    Text key = new Text(decade + " " + w1 + " " + w2);
                    context.write(new Text(decade), occurrences);
                    context.write(key, occurrences);
                }
            }
        }
    }


    /**
     * Input:
     * 1) key = <decade> value = <occurrences>
     * 2) key = <decade w1 w2> value = <occurrences>
     * Output:
     * 1) key = <decade> value = occurrences           (the string in the file decade \t occurrences      (N for this decade))
     * 2) key = decade w1 w2 value = occurrences        (the string in the file decade w1 w2 \t occurrences)
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            long occurrences = 0;
            for (Text value: Values){
                occurrences+= Long.parseLong(value.toString());
            }
            context.write(key, new Text(String.format("%d", occurrences)));
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            long occurrences = 0;
            for (Text value: Values){
                occurrences+= Long.parseLong(value.toString());
            }
            context.write(key, new Text(String.format("%d", occurrences)));
        }
    }

    //partition by the lex order of all the key
    public static class PartitionerClass extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setCombinerClass(CombinerClass.class);
        conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes, more mappers
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://oriori316283464/output_step_1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
