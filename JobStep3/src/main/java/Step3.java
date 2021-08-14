import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

public class Step3 {
    private static final String JOB_NAME = "step 3";
    public static final String DELIMITER = ",";
    private static final String SECOND_STEP_OUTPUT = "s3://dsps212--files/output2";
    private static final String THIRD_STEP_OUTPUT = "s3://dsps212--files/output3";
    private static final String N_TAG = "N:";
    private static final String FIRST_TAG = "First:";
    private static final String SECOND_TAG = "Second:";
    private static final String NGRAM_TAG = "NGram:";
    private static boolean IS_PRINTING;
    private static boolean IS_AGGREGATE;
    private static final String PRINT = "print";

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
            List<String> chunks = new ArrayList<>();
            for (Text value : values) {
                chunks.add(value.toString());
            }
            context.write(key, new Text(String.join(DELIMITER, chunks)));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReducers) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReducers;
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private static final int DECADE_INDEX = 0;
        private static final int NGRAM_INDEX = 1;

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        /**
         * @param decadeNGram a decade NGram string
         * @param counters list of chunks of tagged counters relevant to the decade NGram
         * @param context
         * 198 good boy -> ["N: 279" "First: 29" ["Second: 17" "Ngram: 5"]]
         * Outputs:
         *      key: a string representing the decade and NGram
         *      value: pmi and npmi values for NGram in decade
         */
        @Override
        public void reduce(Text decadeNGram, Iterable<Text> counters, Context context) throws IOException, InterruptedException {
            if(IS_PRINTING){
                System.err.println("red3 in: " + decadeNGram + " -> " + counters);
                System.out.println("red3 in: " + decadeNGram + " -> " + counters);
            }
            long N = -1, first = -1, second = -1, occurrences = -1;
            for (Text value : counters) {
                String[] fields = value.toString().split(DELIMITER);
                for (int i = 0; i < fields.length; i+=2) { // tag+counter pairs
                    long count = Long.parseLong(fields[i + 1]);
                    switch (fields[i]){
                        case N_TAG:
                            N = count;
                            break;
                        case FIRST_TAG:
                            first = count;
                            break;
                        case SECOND_TAG:
                            second = count;
                            break;
                        case NGRAM_TAG:
                            occurrences = count;
                            break;
                    }
                }
            }
            if(IS_PRINTING){
                System.err.printf("data: N=%d c1=%d c2=%d c1c2=%d\n", N, first, second, occurrences);
            }

            if (N == -1 | first == -1 | second == -1 | occurrences == -1){
                throw new IllegalStateException(String.format("missing data: N=%d c1=%d c2=%d c1c2=%d\n", N, first, second, occurrences));
            }
            double pNGram = p(occurrences, N);
            double pmi = Math.log(occurrences) + Math.log(N) - Math.log(first) - Math.log(second);
            double npmi = pmi / -Math.log(pNGram);
            String[] fields = decadeNGram.toString().split(DELIMITER);
            int decade = Integer.parseInt(fields[DECADE_INDEX]);
            if(IS_PRINTING){
                System.err.println("red3 out: " + decade + " -> " + npmi + DELIMITER + fields[NGRAM_INDEX]);
            }
            context.write(new Text(String.valueOf(decade)), new Text(npmi + DELIMITER + fields[NGRAM_INDEX]));
        }

        private double p(Long count, Long countAll){
            return count / (double)countAll;
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
        job.setJarByClass(Step3.class);

        if (IS_AGGREGATE) {
            job.setCombinerClass(CombinerClass.class);
        }

        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(SECOND_STEP_OUTPUT));
        FileOutputFormat.setOutputPath(job, new Path(THIRD_STEP_OUTPUT));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // waiting for job to finish
        boolean jobResult = job.waitForCompletion(true);
        System.exit(jobResult ? 0 : 1);
    }

}
