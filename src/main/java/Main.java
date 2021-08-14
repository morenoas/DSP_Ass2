import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.util.*;


public class Main {
    private static boolean debug = false;
    private static String print = "false";
    private static String aggregation = "false";
    private static String minNPMI;
    private static String minRelNPMI;

    private static void parseArgs(String[] args) {
        minNPMI = args[0];
        minRelNPMI = args[1];
        for (int i = 2; i < args.length; i++) {
            switch (args[i].toLowerCase()) {
                case "-d":
                    debug = true;
                    break;
                case "-p":
                    print = "true";
                    break;
                case "-a":
                    aggregation = "true";
                    break;
            }
        }
    }


    public static void main(String[] args) {
        parseArgs(args);

        List<String> stepArgs = new Vector<>();
        stepArgs.add(aggregation);
        stepArgs.add(print);
        stepArgs.add(minNPMI);
        stepArgs.add(minRelNPMI);

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard().withRegion(Regions.US_EAST_1).build();
        StepConfig stepConfigDebug = new StepConfig()
                .withName("Enable Debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new HadoopJarStepConfig()
                        .withJar("command-runner.jar")
                        .withArgs("state-pusher-script"));
//        steps declarations
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://dsps212--artifacts/jars/Ass2/JobStep1.jar") // This should be a full map reduce application.
                .withMainClass("Step1")
                .withArgs(stepArgs);
        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://dsps212--artifacts/jars/Ass2/JobStep2.jar") // This should be a full map reduce application.
                .withMainClass("Step2")
                .withArgs(stepArgs);
        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://dsps212--artifacts/jars/Ass2/JobStep3.jar") // This should be a full map reduce application.
                .withMainClass("Step3")
                .withArgs(stepArgs);
        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://dsps212--artifacts/jars/Ass2/JobStep4.jar") // This should be a full map reduce application.
                .withMainClass("Step4")
                .withArgs(stepArgs);
        StepConfig stepConfig4 = new StepConfig()
                .withName("step4")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar("s3n://dsps212--artifacts/jars/Ass2/JobStep5.jar") // This should be a full map reduce application.
                .withMainClass("Step5")
                .withArgs(stepArgs);
        StepConfig stepConfig5 = new StepConfig()
                .withName("step5")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M5_XLARGE.toString())
                .withSlaveInstanceType(InstanceType.M5_XLARGE.toString())
                .withHadoopVersion("2.6.0")
//                .withEc2KeyName("DSP_key")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        List<StepConfig> steps = Arrays.asList(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5);
        if (debug) {
            List<StepConfig> newSteps = new ArrayList<>();
            newSteps.add(0, stepConfigDebug);
            newSteps.addAll(steps);
            steps = newSteps;
        }

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DSP-ASS2")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.16.0")
                .withSteps(steps)
                .withInstances(instances)
//                .withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5)
                .withLogUri("s3n://dsps212--artifacts/logs/");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
//       TODO:
//          1. wait on step5 queue to get a success message.
//          2. download final output/s from s3.
//          2. delete logs and outputs from s3.
    }
}

/*
STEP 1
map
<- {lineID -> w1w2, c(w1w2 in year), year}
-> {decade, w1w2 -> c(w1w2 in year)}
reduce
<- {decade, w1w2 -> [c(w1w2 in year)]}
    compute c(w1w2 in decade)
-> {decade, w1w2 -> c(w1w2 in decade)}

STEP 2
map
<- {decade, w1w2 -> c(w1w2 in decade)}
-> {decade, w1 -> c(w1w2), w1w2} | {decade, w2 -> c(w1w2), w1w2} | {decade -> c(w1w2), w1w2} | {decade, w1w2 -> c(w1w2), w1w2}
   {decade, w1, * -> c(w1w2)} | {decade, w2, * -> c(w1w2)} | {decade, * -> c(w1w2)} | {decade, w1w2, * -> c(w1w2)}
reduce
<- {decade, w1 -> [c(w1w2), w2], w1w2]} | {decade, w2 -> [c(w1w2), w1w2]} | {decade -> [c(w1w2), w1w2]} | {decade, w1w2 -> [c(w1w2), w1w2]}
    compute [c(w1) | c(w2) | N | c(w1w2)] same for all w1w2 in values
-> [{decade, w1w2 -> c(w1) | c(w2) | N | c(w1w2)}]

Step 3
reduce
<- decade, w1w2 -> [c(w1w2), c(w1), c(w2), N]
    compute npmi
-> decade -> npmi, w1w2

Step 4
reduce
<- decade -> [npmi, w1w2]
    compute sum npmi
-> [decade -> w1w2, npmi, sumNPMI]

Step 5
map
<- decade -> w1w2, npmi, sumNPMI
    filter collocations by minPMI and relMinPMI
-> decade -> w1w2, npmi
reduce
<- decade -> [w1w2, pmi, npmi]
    return input as is
 */