import com.google.cloud.storage.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.RasterFormatException;
import java.io.*;
import java.util.Map;

public class LoadImageFn extends DoFn<PubsubMessage, String> {

    @ProcessElement
    public void processElement(@Element PubsubMessage m, OutputReceiver<String> out) {
        Map<String, String> attr = m.getAttributeMap();

        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blob = BlobId.of(attr.get("bucketId"), attr.get("objectId"));
        byte[] content = storage.readAllBytes(blob);
        InputStream is = new ByteArrayInputStream(content);

        BufferedImage img = null;
        try {
            img = ImageIO.read(is);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 切り取り始める座標
        int X = 50;
        int Y = 50;
        // 切り取るサイズ
        int W = 100;
        int H = 100;

        BufferedImage subimg;  // 切り出し画像格納クラス
        try {
            assert img != null;
            subimg = img.getSubimage(X, Y, W, H);
        }
        catch ( RasterFormatException re ) {
            System.out.println( "指定した範囲が画像の範囲外です" );
            return;
        }

        BlobId blobId = BlobId.of(attr.get("bucketId"), "result/cropped_image.jpg");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("image/jpeg").build();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BufferedOutputStream os = new BufferedOutputStream( bos );

        try {
            ImageIO.write(subimg, "jpeg", os);
        } catch (IOException e) {
            e.printStackTrace();
        }

        storage.create(blobInfo, bos.toByteArray());

        out.output(String.valueOf(img.getHeight()));
    }

}
