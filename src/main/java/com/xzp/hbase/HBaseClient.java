package com.xzp.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xzp.AbstractClient;
import com.xzp.curve.XZ2SFC;
import com.xzp.curve.XZPlusSFC;
import com.xzp.geometry.MinimumBoundingBox;
import com.xzp.utils.ByteArrays;
import com.xzp.utils.WKTUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.sfcurve.IndexRange;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-08-20 16:43
 * @modified by :
 **/
public class HBaseClient implements AbstractClient, Closeable {
    private Short precise;
    private Admin admin;
    private HTable hTable;
    private String tableName;
    private static String DEFAULT_CF = "cf";
    private static String DEFAULT_COL = "v";
    private static String DEFAULT_ID = "nid";
    private static Short P = 16;
    private static Short shard = 4;
    private XZ2SFC sfc;
    private int BATCH_SIZE = 10000;
    private List<Put> putCache;
    private int putSize = 0;
    private Connection connection;

    public HBaseClient(String tableName) throws IOException {
        this(tableName, P);
    }

    public HBaseClient(String tableName, Short precise) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.tableName = tableName;
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (!admin.tableExists(table.getTableName())) {
            create();
        }
        this.hTable = new HTable(TableName.valueOf(tableName), connection);
        this.precise = precise;
        this.sfc = XZPlusSFC.apply(precise);
    }

    public HBaseClient(String tableName, Short precise, XZ2SFC sfc) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.tableName = tableName;
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (!admin.tableExists(table.getTableName())) {
            create();
        }
        this.hTable = new HTable(TableName.valueOf(tableName), connection);
        this.precise = precise;
        this.sfc = sfc;
    }

    @Override
    public void create() throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        table.addFamily(new HColumnDescriptor(DEFAULT_CF).setCompressionType(Compression.Algorithm.GZ));
        admin.createTable(table);
    }

    @Override
    public void insert(String id, String geom, String value) throws IOException {
        hTable.put(getPut(id, geom, value));
    }

    public Put getPut(String id, String geom, String value) throws IOException {
        Geometry geometry = WKTUtils.read(geom);
        assert geometry != null;
        Envelope bbox = geometry.getEnvelopeInternal();
        Long index = this.sfc.index(bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), false);
        short s = (short) (index % shard);
        byte[] bytes = new byte[9 + id.length()];
        bytes[0] = (byte) s;
        //Bytes.toBytes(s)
        ByteArrays.writeLong(index, bytes, 1);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, 9, id.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_ID), Bytes.toBytes(id));
        return put;
    }

    public void batchInsert(String id, String geom, String value) throws IOException {
        //TODO
        if (null == this.putCache) {
            this.putCache = new ArrayList<>(BATCH_SIZE);
        }
        this.putCache.add(getPut(id, geom, value));
        putSize++;
        if (putSize == BATCH_SIZE) {
            hTable.put(this.putCache);
            this.putCache.clear();
            putSize = 0;
        }
    }

    public void finishBatchPut() throws IOException {
        if (null != this.putCache && !this.putCache.isEmpty()) {
            hTable.put(this.putCache);
            this.putCache.clear();
            putSize = 0;
        }
    }

    @Override
    public List<Result> rangeQuery(Double minLng, Double minLat, Double maxLng, Double maxLat) throws IOException, InterruptedException {
        Iterator<Iterable<IndexRange>> rs = this.sfc.ranges(minLng, minLat, maxLng, maxLat).sliding(10, 10);
        //Iterator<IndexRange> rss = this.sfc.ranges(minLng, minLat, maxLng, maxLat).toIterator();
        //System.out.println("range size:"+rss.size());
        Geometry mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat).toPolygon(4326);
        final List<Result> resultList = new ArrayList<>();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("demo-pool-%d").build();
        ExecutorService singleThreadPool = new ThreadPoolExecutor(10, 20,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        long time = System.currentTimeMillis();
        int size = 1;
        int rangeSize = 0;
//        while (rss.hasNext()) {
//            rangeSize++;
//            IndexRange range = rss.next();
//            for (int i = 0; i < shard; i++) {
//                byte[] startRow = new byte[9];
//                byte[] endRow = new byte[9];
//                startRow[0] = (byte) i;
//                endRow[0] = (byte) i;
//                ByteArrays.writeLong(range.lower(), startRow, 1);
//                ByteArrays.writeLong(range.upper() + 1L, endRow, 1);
//                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
//                scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL));
//                scan.setCaching(200);
//                //scan.setFilter(new SpatialFilter(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_CF), WKTUtils.read(mbr.toText())));
//                ResultScanner resultScanner = null;
//                try {
//                    resultScanner = hTable.getScanner(scan);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                for (Result res : resultScanner) {
//                    if (!range.contained()) {
//                        Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL))));
//                        if (geo.intersects(mbr)) {
//                            resultList.add(res);
//                        }
//                    } else {
//                        resultList.add(res);
//                    }
//                    //resultList.add(res);
//                    size++;
//                }
//                resultScanner.close();
//            }
//        }
        while (rs.hasNext()) {
            Iterator<IndexRange> ranges = rs.next().toIterator();
            singleThreadPool.execute(() -> {
                while (ranges.hasNext()) {
                    try (HTable table = new HTable(TableName.valueOf(tableName), connection)) {
                        IndexRange range = ranges.next();
                        for (int i = 0; i < shard; i++) {
                            byte[] startRow = new byte[9];
                            byte[] endRow = new byte[9];
                            startRow[0] = (byte) i;
                            endRow[0] = (byte) i;
                            ByteArrays.writeLong(range.lower(), startRow, 1);
                            ByteArrays.writeLong(range.upper() + 1L, endRow, 1);
                            Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
                            scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL));
                            scan.setCaching(200);
                            //scan.setFilter(new SpatialFilter(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_CF), WKTUtils.read(mbr.toText())));
                            ResultScanner resultScanner = null;
                            try {
                                resultScanner = table.getScanner(scan);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            for (Result res : resultScanner) {
                                if (!range.contained()) {
                                    Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL))));
                                    if (geo.intersects(mbr)) {
                                        resultList.add(res);
                                    }
                                } else {
                                    resultList.add(res);
                                }
                                //resultList.add(res);
                            }
                            resultScanner.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

//        while (xzRanges.hasNext()) {
//            long t1 = System.currentTimeMillis();
//            IndexRange range = xzRanges.next();
//            byte[] startRow = new byte[9];
//            byte[] endRow = new byte[9];
//            singleThreadPool.execute(() -> {
//                startRow[0] = (byte) 1;
//                endRow[0] = (byte) 1;
//                ByteArrays.writeLong(range.lower(), startRow, 1);
//                ByteArrays.writeLong(range.upper(), endRow, 1);
//                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
//                scan.setCaching(200);
//                ResultScanner resultScanner = null;
//                try {
//                    resultScanner = hTable.getScanner(scan);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                for (Result res : resultScanner) {
//                    resultList.add(res);
//                }
//            });
//
//            singleThreadPool.execute(() -> {
//                startRow[0] = (byte) 2;
//                endRow[0] = (byte) 2;
//                ByteArrays.writeLong(range.lower(), startRow, 1);
//                ByteArrays.writeLong(range.upper(), endRow, 1);
//                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
//                scan.setCaching(200);
//                ResultScanner resultScanner = null;
//                try {
//                    resultScanner = hTable.getScanner(scan);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                for (Result res : resultScanner) {
//                    resultList.add(res);
//                }
//            });
//            singleThreadPool.execute(() -> {
//                startRow[0] = (byte) 3;
//                endRow[0] = (byte) 3;
//                ByteArrays.writeLong(range.lower(), startRow, 1);
//                ByteArrays.writeLong(range.upper(), endRow, 1);
//                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
//                scan.setCaching(200);
//                ResultScanner resultScanner = null;
//                try {
//                    resultScanner = hTable.getScanner(scan);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                for (Result res : resultScanner) {
//                    resultList.add(res);
//                }
//            });
//            singleThreadPool.execute(() -> {
//                startRow[0] = (byte) 4;
//                endRow[0] = (byte) 4;
//                ByteArrays.writeLong(range.lower(), startRow, 1);
//                ByteArrays.writeLong(range.upper(), endRow, 1);
//                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
//                scan.setCaching(200);
//                ResultScanner resultScanner = null;
//                try {
//                    resultScanner = hTable.getScanner(scan);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                for (Result res : resultScanner) {
//                    resultList.add(res);
//                }
//            });
////            for (int i = 1; i <= shard; i++) {
////                startRow[0] = (byte) i;
////                endRow[0] = (byte) i;
////                ByteArrays.writeLong(range.lower(), startRow, 1);
////                ByteArrays.writeLong(range.upper(), endRow, 1);
////                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
////                scan.setCaching(200);
////                ResultScanner resultScanner = hTable.getScanner(scan);
////                for (Result res : resultScanner) {
////                    resultList.add(res);
////                }
////            }
//            System.out.println(System.currentTimeMillis() - t1);
//        }
        //singleThreadPool.shutdown();
        //singleThreadPool.awaitTermination(3, TimeUnit.SECONDS);
//        while (!singleThreadPool.isTerminated()) {
//
//        }
        singleThreadPool.shutdown();
        singleThreadPool.awaitTermination(3, TimeUnit.SECONDS);
        System.out.println(System.currentTimeMillis() - time);
        System.out.println("size:" + size);
        System.out.println("rang size:" + rangeSize);
        return resultList;
    }


    @Override
    public void update(String id, String geom, String value) {

    }

    @Override
    public void delete(String id) {

    }

    @Override
    public void close() throws IOException {
        admin.close();
        hTable.close();
        connection.close();
    }
}
