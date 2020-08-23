package com.xzp.hbase.example;

import com.xzp.geometry.MinimumBoundingBox;
import com.xzp.hbase.HBaseClient;
import com.xzp.hbase.RtreeHBaseClient;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-08-23 16:15
 * @modified by :
 **/
public class RtreeExample {
    public static void main(String[] args) {
        String[] mbr = "106.65618,26.61497,106.68118,26.63997".split(",");
        String tableR = "r";
        double lat = 26.21497;
        double lon = 106.25618;
        try (RtreeHBaseClient hBaseClient = new RtreeHBaseClient(tableR)) {
            Random random = new Random(1000000);
            Random randomLat = new Random(2661497);
            for (int k = 0; k < 50000; k++) {
                for (int j = 1; j <= 5; j++) {
                    double offset = random.nextDouble() * 0.5;
                    double offsetLat = randomLat.nextDouble() * 0.5;
                    //System.out.println(String.format("%s_%s", offset, m));
                    MinimumBoundingBox mbr2 = new MinimumBoundingBox(lon + offset, lat + offsetLat, lon + offset + j * 0.005, lat + offsetLat + j * 0.005);
                    //System.out.println(String.format("%s", mbr2.toPolygon(4326).toText()));
                    hBaseClient.batchInsert((k * 5 + j) + "", mbr2.toPolygon(4326).toText(), mbr2.toPolygon(4326).toText());
                }
            }
            hBaseClient.finishBatchPut();
            long time = System.currentTimeMillis();
            List<Result> resultList = hBaseClient.rangeQuery(106.64618, 26.60497, 106.69118, 26.64997);
            System.out.println(resultList.size());
            System.out.println(System.currentTimeMillis() - time);
            System.out.println("----------");
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
