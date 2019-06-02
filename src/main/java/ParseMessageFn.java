import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Map;

public class ParseMessageFn extends DoFn<PubsubMessage, String> {

    @ProcessElement
    public void processElement(@Element PubsubMessage m, OutputReceiver<String> out) {
        Map<String, String> attr = m.getAttributeMap();
        if (attr.get("eventType").equals("OBJECT_FINALIZE")) {
            String res = attr.get("bucketId") + "" + attr.get("objectId");
            out.output(res);
        }
        out.output("");
    }

}
