import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
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


public class Step2 {
    private static final String JOB_NAME = "step 2";
    private static final String DELIMITER = ",";
    private static final String NGRAM_DELIMITER = " ";
    private static final int NGRAM_INDEX = 1;
    private static final String FIRST_STEP_OUTPUT = "s3://dsps212--files/output1";
    private static final String SECOND_STEP_OUTPUT = "s3://dsps212--files/output2";
    private static final String N_TAG = "N:";
    private static final String FIRST_TAG = "First:";
    private static final String SECOND_TAG = "Second:";
    private static final String NGRAM_TAG = "NGram:";
    private static boolean IS_PRINTING;
    private static boolean IS_AGGREGATE;
    private static final String PRINT = "print";


    public static class MyTextComparator extends WritableComparator {

        public MyTextComparator() {
            super(Text.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2) *(-1);
        }
    }


    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private static final int DECADE_INDEX = 0;

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

//        occurrences = total times this NGram occur in this decade.
        @Override
        public void map(Text key, Text occurrences, Context context) throws IOException, InterruptedException {
            if(IS_PRINTING){
                System.err.println("map2 in: " + key.toString() + " -> " + occurrences);
            }

            String[] fields = key.toString().split(DELIMITER);
            String NGram = fields[NGRAM_INDEX];

            int decade;
            try {
                decade = Integer.parseInt(fields[DECADE_INDEX]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }

            // Generates 4 unique messages per NGram (per decade): "First: c(w1)", "Second: c(w2)", "NGram: c(w1w2)", "N: N"
            String[] words = NGram.split(NGRAM_DELIMITER);
            if(IS_PRINTING){
                System.err.println("map2 out: " + decade + DELIMITER + NGram +"\t"+ String.join(",", words) + occurrences);
            }
            context.write(new Text(N_TAG + DELIMITER + decade + DELIMITER + "_"+ DELIMITER),
                          new Text(NGram));
            context.write(new Text(FIRST_TAG + DELIMITER + decade + DELIMITER + words[0]+ DELIMITER),
                          new Text(NGram));
            context.write(new Text(SECOND_TAG + DELIMITER + decade + DELIMITER + words[1]+ DELIMITER),
                          new Text(NGram));
            context.write(new Text(NGRAM_TAG + DELIMITER + decade + DELIMITER + NGram+ DELIMITER),
                          new Text(NGram));

            // the same keys with a "*" in order to get rid of the first for loop in the reduce.
            context.write(new Text(N_TAG + DELIMITER + decade + DELIMITER + "_" + DELIMITER + "*"),
                          occurrences);
            context.write(new Text(FIRST_TAG + DELIMITER + decade + DELIMITER + words[0] + DELIMITER + "*"),
                          occurrences);
            context.write(new Text(SECOND_TAG + DELIMITER + decade + DELIMITER + words[1] + DELIMITER + "*"),
                          occurrences);
            context.write(new Text(NGRAM_TAG + DELIMITER + decade + DELIMITER + NGram + DELIMITER + "*"),
                          occurrences);
        }
    }


    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        /**
         * @param key a tagged string: N, First, Second, NGram
         * @param values num of occurrences and NGram
         * @param context
         *
         * Outputs:
         *      key: the tagged string
         *      value: the partial sum of occurrences, with a delimiter separated list of NGrams
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long partialSum = 0;
            List<String> NGrams = new ArrayList<>();
            boolean occurrences = true;
            for (Text value : values) {
                try {
                    partialSum += Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    occurrences = false;
                    NGrams.add(value.toString());
                }
            }
            if (occurrences){
                context.write(key, new Text(String.valueOf(partialSum)));
            }
            else {
                context.write(key, new Text(String.join(DELIMITER, NGrams)));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReducers) {
            String[] fields = key.toString().split(DELIMITER);
            String[] newFields = Arrays.copyOfRange(fields, 0, 3);
            String newKey = String.join(DELIMITER, newFields);
            return (newKey.hashCode() & Integer.MAX_VALUE) % numReducers;
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private static final int TAG_INDEX = 0;
        private static final int DECADE_INDEX = 1;

        long finalSum = 0;  // global

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        /***
         * @param key a tagged string: N, First, Second, NGram, keyed by decade and property
         * @param values list of
         * @param context
         *
         * Outputs:
         *      key: NGram decade string
         *      value: tagged sum of occurrences for the input tag
         */@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(IS_PRINTING){
                System.err.println("red2 in: " + key + " -> " + values.toString());
            }
            String[] keyFields = key.toString().split(DELIMITER);
            for (Text value : values) {
                if(keyFields.length == 4){  // means there is a "*" in the key
                    finalSum += Long.parseLong(value.toString());
                    if(IS_PRINTING){
                        System.err.println("red2: " + key + " -> " + finalSum);
                    }
                }
                else{
                    String[] valueFields = value.toString().split(DELIMITER);
                    for (String valueField : valueFields) {
                        if (IS_PRINTING) {
                            System.err.println("red2 out: " + key + " -> " + valueField + DELIMITER + finalSum);
                        }
                        context.write(new Text(keyFields[DECADE_INDEX] + DELIMITER + valueField),
                                      new Text(keyFields[TAG_INDEX] + DELIMITER + finalSum));
                    }
                }
            }
            if(keyFields.length != 4){  // means there is no "*" in the key
                finalSum = 0;
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
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setSortComparatorClass(MyTextComparator.class);

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

        FileInputFormat.addInputPath(job, new Path(FIRST_STEP_OUTPUT));
        FileOutputFormat.setOutputPath(job, new Path(SECOND_STEP_OUTPUT));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // waiting for job to finish
        boolean jobResult = job.waitForCompletion(true);
        System.exit(jobResult ? 0 : 1);
    }

}
