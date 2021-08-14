import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Step4 {
    private static final String JOB_NAME = "step 4";
    public static final String DELIMITER = ",";
    private static final String THIRD_STEP_OUTPUT = "s3://dsps212--files/output3";
    private static final String FORTH_STEP_OUTPUT = "s3://dsps212--files/output4";
    private static final int NPMI_INDEX = 0;
    private static boolean IS_PRINTING;
    private static boolean IS_AGGREGATE;
    private static final String PRINT = "print";


    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        //        occurrences = total times this NGram occur in this decade.
        @Override
        public void map(Text decade, Text npmiNgram, Context context) throws IOException, InterruptedException {
            if(IS_PRINTING){
                System.err.println("map4 in: " + decade.toString() + " -> " + npmiNgram);
            }
            context.write(decade, npmiNgram);
            // the same keys with a "-0.5" in order to get rid of the first for loop in the reduce.
            // the ordered keys coming to reduce are in an ascending order.
            double newKey = Double.parseDouble(decade.toString())-0.5;
            String npmi = npmiNgram.toString().split(DELIMITER)[NPMI_INDEX];
            context.write(new Text(String.valueOf(newKey)), new Text(npmi));
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double localSumNpmi = 0;
            List<String> chunks = new ArrayList<>();
            boolean isFraction = true;
            double decade = Double.parseDouble(key.toString());
            for (Text value : values) {
                if(decade % 1.0 != 0){
                    localSumNpmi += Double.parseDouble(value.toString());
                }
                else{
                    isFraction = false;
                    chunks.add(value.toString());
                }
            }
            if (isFraction) { // if decade is a fraction
                context.write(key, new Text(String.valueOf(localSumNpmi)));
            }
            else{
                context.write(key, new Text(String.join(DELIMITER,chunks)));
            }
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReducers) {
            double newKey = Double.parseDouble(key.toString());
            double decade = Math.ceil(newKey);
            return  (int) decade % numReducers;
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        double finalSumNpmi = 0;

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        /**
         * @param key the specific decade
         * @param values list of npmi (partial sum) followed by a list of NGram+npmi pairs
         * @param context
         * 197 -> [{6.3, good boy}, {4.2, bad boy}, {19.35, 5.4 good dog, 14.95 bad dog}]
         * Outputs:
         *      key: decade+NGram
         *      value: the partial sum of occurrences, with a delimiter separated list of NGrams
         */

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double decade = Double.parseDouble(key.toString());
            if(IS_PRINTING){
                System.err.println("red4 in: " + key + " -> " + values);
                System.err.println("decade -> " + decade);
            }
            for (Text value : values) {
                if (decade % 1.0 != 0) { // if decade is a fraction
                    double npmi = Double.parseDouble(value.toString());
                    finalSumNpmi += npmi;
                    System.err.println("red4: " + key + " -> " + npmi + finalSumNpmi);
                    if(IS_PRINTING){
                        System.err.println("red4: " + key + " -> " + finalSumNpmi);
                    }
                }
                else { // case decade is the real decade
                    String[] valueFields = value.toString().split(DELIMITER);
                    for (int i = 0; i < valueFields.length; i+=2) { // npmi+ngram pairs
                        String npmi = valueFields[i];
                        String NGram = valueFields[i+1];
                        StringBuilder valueString = new StringBuilder()
                                .append(NGram)
                                .append(DELIMITER)
                                .append(finalSumNpmi)
                                .append(DELIMITER)
                                .append(npmi);
                        if(IS_PRINTING){
                            System.err.println("red4 out: " + decade + " -> " + valueString);
                        }
                        context.write(new Text(String.valueOf((int)decade*10)), new Text(valueString.toString()));
                    }
                }
            }
            if (decade % 1.0 == 0) { // if decade is the real decade
                finalSumNpmi = 0;
            }
        }


    }

    private static void parseArgs(String[] args) {
        if (args.length < 3) {
            return;
        }
        IS_AGGREGATE = Boolean.parseBoolean(args[1]);
        System.err.printf("aggregate: %b\n", IS_AGGREGATE);
        IS_PRINTING = Boolean.parseBoolean(args[2]);
        System.err.printf("print: %b\n", IS_PRINTING);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.err.printf("------- Start %s -------\n\n%n", JOB_NAME);
        System.err.printf("args = %s\n", Arrays.toString(args));
        parseArgs(args);
        Configuration conf = new Configuration();
        conf.setBoolean(PRINT, IS_PRINTING);
        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        if (IS_AGGREGATE) {
            job.setCombinerClass(CombinerClass.class);
        }

        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(THIRD_STEP_OUTPUT));
        FileOutputFormat.setOutputPath(job, new Path(FORTH_STEP_OUTPUT));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // waiting for job to finish
        boolean jobResult = job.waitForCompletion(true);
        System.exit(jobResult ? 0 : 1);
    }

}


