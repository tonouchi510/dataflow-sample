import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;


public class PubsubToText {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        dataflowOptions.setRunner(DataflowRunner.class);
        dataflowOptions.setProject("my-project");
        dataflowOptions.setStagingLocation("gs://dataflow-sample-bucket/staging");
        dataflowOptions.setTemplateLocation("gs://dataflow-sample-bucket/templates/MyTemplate");
        dataflowOptions.setStreaming(true);
        dataflowOptions.setNumWorkers(1);

        run(dataflowOptions);
    }

    public static PipelineResult run(DataflowPipelineOptions options) {
        String topic = "projects/dataflow-sample/topics/sample";
        String output = "gs://dataflow-sample-result/output.txt";

        Pipeline p = Pipeline.create(options);

        /*
         * Steps:
         *   1) Read string messages from PubSub
         *   2) Window the messages into minute intervals specified by the executor.
         *   3) Output the windowed files to GCS
         */
        p.apply("Read PubSub Events", PubsubIO.readMessagesWithAttributes().fromTopic(topic))
                .apply( "60s Window",
                        Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                .apply("Load Image", ParDo.of(new LoadImageFn()))
                .apply("Write File(s)", TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(1)
                        .to(output));

        return p.run();
    }
}