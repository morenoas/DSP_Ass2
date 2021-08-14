import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class Step1 {
    private static final String JOB_NAME = "step 1";
    private static final String PATH_TO_CORPUS = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
    public static final String LINE_DELIMITER = "\t";
    public static final String KEY_DELIMITER = ",";
    public static final String NGRAM_DELIMITER = " ";
    public static final int NGRAM_INDEX = 0;
    public static final int YEAR_INDEX = 1;
    public static final int OCCURRENCES_INDEX = 2;
    private static final String FIRST_STEP_OUTPUT = "s3://dsps212--files/output1";
    private static boolean IS_PRINTING;
    private static boolean IS_AGGREGATE;
    private static final String PRINT = "print";

    private static final Pattern regexp = Pattern.compile("^.[\u05D0-\u05EA]+$");


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final Set<String> stopWords = new HashSet<>(
                Arrays.asList(
                        "אני", "את", "אתה", "אנחנו", "אתן", "אתם", "הם", "הן", "היא", "הוא", "שלי", "שלו", "שלך","שלה","שלנו","שלכם","שלכן","שלהם","שלהן","לי","לו","לה","לנו","לכם","לכן","להם","להן","אותה","אותו","זה","זאת","אלה","אלו","תחת","מתחת","מעל","בין","עם","עד","נגר","על","אל","מול","של","אצל","כמו","אחר","אותו","בלי","לפני","אחרי","מאחורי","עלי","עליו","עליה","עליך","עלינו","עליכם","לעיכן","עליהם","עליהן","כל","כולם","כולן","כך","ככה","כזה","זה","זות","אותי","אותה","אותם","אותך","אותו","אותן","אותנו","ואת","את","אתכם","אתכן","איתי","איתו","איתך","איתה","איתם","איתן","איתנו","איתכם","איתכן","יהיה","תהיה","היתי","היתה","היה","להיות","עצמי","עצמו","עצמה","עצמם","עצמן","עצמנו","עצמהם","עצמהן","מי","מה","איפה","היכן","במקום שבו","אם","לאן","למקום שבו","מקום בו","איזה","מהיכן","איך","כיצד","באיזו מידה","מתי","בשעה ש","כאשר","כש","למרות","לפני","אחרי","מאיזו סיבה","הסיבה שבגללה","למה","מדוע","לאיזו תכלית","כי","יש","אין","אך","מנין","מאין","מאיפה","יכל","יכלה","יכלו","יכול","יכולה","יכולים","יכולות","יוכלו","יוכל","מסוגל","לא","רק","אולי","אין","לאו","אי","כלל","נגד","אם","עם","אל","אלה","אלו","אף","על","מעל","מתחת","מצד","בשביל","לבין","באמצע","בתוך","דרך","מבעד","באמצעות","למעלה","למטה","מחוץ","מן","לעבר","מכאן","כאן","הנה","הרי","פה","שם","אך","ברם","שוב","אבל","מבלי","בלי","מלבד","רק","בגלל","מכיוון","עד","אשר","ואילו","למרות","אס","כמו","כפי","אז","אחרי","כן","לכן","לפיכך","מאד","עז","מעט","מעטים","במידה","שוב","יותר","מדי","גם","כן","נו","אחר","אחרת","אחרים","אחרות","אשר","או"
                ));

        protected void setup(Context context) {
            IS_PRINTING = context.getConfiguration().getBoolean(PRINT, false);
        }

        int recCount = 0;
        int NCount = 0;
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

            if(IS_PRINTING) {
                System.err.println("map1 in: " + line);
            }

            String[] fields = line.toString().split(LINE_DELIMITER);
            String NGram = fields[NGRAM_INDEX];
            int year;
            int occurrences;
            try {
                year = Integer.parseInt(fields[YEAR_INDEX]);
                occurrences = Integer.parseInt(fields[OCCURRENCES_INDEX]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }
            int decade = year/10;
            String[] words = NGram.split(NGRAM_DELIMITER);
            if (words.length != 2 || notValidNGram(words)) {
                return;
            }
            if(IS_PRINTING){
                System.err.println("map1 out: " + decade + KEY_DELIMITER + NGram + LINE_DELIMITER + occurrences);
            }
            context.write(new Text(decade + KEY_DELIMITER + NGram),
                          new Text(String.valueOf(occurrences)));
            if(IS_PRINTING){
                recCount++; NCount += occurrences;
                System.err.println("so far N= " + NCount);
            }
        }

        private boolean notValidNGram(String[] words) {
            for (String word : words) {
                if (!word.matches(regexp.pattern()) || stopWords.contains(word)) {
                    if(IS_PRINTING){
                        System.err.println(word + " is invalid!!!");
                    }
                    return true;
                }
            }
            return false;
        }
    }


    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        /**
         * @param key a string representing the decade and NGram
         * @param values num of occurrences in a specific year
         * @param context
         *
         * Outputs:
         *      key: a string representing the decade and NGram
         *      value: the number of occurrences of that NGram in that decade
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text count, int numReducers) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReducers;
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        /**
         * @param key a string representing the decade and NGram
         * @param values list of the num of occurrences in a specific year (without aggregation)
         *               or decade (with aggregation)
         * @param context
         *
         * Outputs:
         *      key: a string representing the decade and NGram
         *      value: the number of occurrences of that NGram in that decade
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
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
        System.err.printf("------- Start %s -------\n\n", JOB_NAME);
        System.err.printf("args = %s\n", Arrays.toString(args));
        parseArgs(args);
        Configuration conf = new Configuration();
        conf.setBoolean(PRINT, IS_PRINTING);
        Job job = Job.getInstance(conf, JOB_NAME);
        job.setJarByClass(Step1.class);
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

        FileInputFormat.addInputPath(job, new Path(PATH_TO_CORPUS));
        FileOutputFormat.setOutputPath(job, new Path(FIRST_STEP_OUTPUT));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // waiting for job to finish
        boolean jobResult = job.waitForCompletion(true);
        System.exit(jobResult ? 0 : 1);
    }

}
