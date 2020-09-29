package com.xzp.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xzp.AbstractClient;
import com.xzp.curve.XZ2SFC;
import com.xzp.curve.XZPlusSFC;
import com.xzp.curve.XZSSFC;
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
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    private int BATCH_SIZE = 100001;
    private List<Put> putCache;
    private int putSize = 0;
    private Connection connection;
    private boolean printLogs = true;

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

    public void setPrintLogs(boolean printLogs) {
        this.printLogs = printLogs;
    }

    @Override
    public void create() throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        table.addFamily(new HColumnDescriptor(DEFAULT_CF).setCompressionType(Compression.Algorithm.SNAPPY));
        admin.createTable(table);
    }

    @Override
    public void insert(String id, String geom, String value) throws IOException {
        hTable.put(getPut(id, geom, value));
    }

    public Put getPut(String id, String geom, String value) {
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

    public Put getXZSPut(String id, String geom, String value) {
        XZSSFC xzssfc = XZSSFC.apply(precise);
        Geometry geometry = WKTUtils.read(geom);
        assert geometry != null;
        Envelope bbox = geometry.getEnvelopeInternal();
        Tuple2<Long, Long> index = xzssfc.indexPositionCode(bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), false);
        short s = (short) (index._1 % shard);
        byte[] bytes = new byte[10 + id.length()];
        bytes[0] = (byte) s;
        bytes[9] = index._2.byteValue();
        //Bytes.toBytes(s)
        ByteArrays.writeLong(index._1, bytes, 1);
        System.arraycopy(Bytes.toBytes(id), 0, bytes, 10, id.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_ID), Bytes.toBytes(id));
        return put;
    }

    public void sbatchInsert(String id, String geom, String value) throws IOException {
        //TODO
        if (null == this.putCache) {
            this.putCache = new ArrayList<>(BATCH_SIZE);
        }
        this.putCache.add(getXZSPut(id, geom, value));
        putSize++;
        if (putSize == BATCH_SIZE) {
            hTable.put(this.putCache);
            this.putCache.clear();
            putSize = 0;
        }
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

    public void batchInsert(List<Put> puts) throws IOException {
        List<Put> putList = new ArrayList<>(Math.min(BATCH_SIZE, puts.size()));
        for (int i = 0; i < puts.size(); i++) {
            putList.add(puts.get(i));
            if ((i + 1) % BATCH_SIZE == 0) {
                hTable.put(putList);
                putList.clear();
            }
        }

        if (!putList.isEmpty()) {
            hTable.put(putList);
            putList.clear();
        }
    }

    public List<Result> singleRangeQuery(Double minLng, Double minLat, Double maxLng, Double maxLat) {
        //Iterator<Iterable<IndexRange>> rs = this.sfc.ranges(minLng, minLat, maxLng, maxLat).sliding(10, 10);
        Iterator<IndexRange> rss = this.sfc.ranges(minLng, minLat, maxLng, maxLat).toIterator();
        //System.out.println("range size:"+rss.size());
        Geometry mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat).toPolygon(4326);
        final List<Result> resultList = new ArrayList<>();
        long time = System.currentTimeMillis();
        int size = 1;
        int rangeSize = 0;
        while (rss.hasNext()) {
            rangeSize++;
            IndexRange range = rss.next();
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
                    resultScanner = hTable.getScanner(scan);
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
                    size++;
                }
                resultScanner.close();
            }
        }
        System.out.println(System.currentTimeMillis() - time);
        System.out.println("size:" + size);
        System.out.println("rang size:" + rangeSize);
        return resultList;
    }

    public List<Result> SRangeQuery(Double minLng, Double minLat, Double maxLng, Double maxLat) {
        //Iterator<Iterable<IndexRange>> rs = this.sfc.ranges(minLng, minLat, maxLng, maxLat).sliding(10, 10);
        long time = System.currentTimeMillis();
        XZSSFC xzssfc = XZSSFC.apply(precise);
        List<Tuple3<IndexRange, Long, Long>> rss = xzssfc.ranges2(minLng, minLat, maxLng, maxLat);
        long indexTime = System.currentTimeMillis() - time;
        //System.out.println("range size:"+rss.size());
        Geometry mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat).toPolygon(4326);
        final List<Result> resultList = new ArrayList<>();
        AtomicInteger size = new AtomicInteger(0);
        AtomicInteger totalSize = new AtomicInteger(0);
        AtomicInteger rangeSize = new AtomicInteger(0);
        for (Tuple3<IndexRange, Long, Long> ranges : rss) {
            rangeSize.addAndGet(1);
            IndexRange range = ranges._1();

            for (int i = 0; i < shard; i++) {
                byte[] startRow = new byte[10];
                byte[] endRow = new byte[10];
                startRow[0] = (byte) i;
                endRow[0] = (byte) i;
                ByteArrays.writeLong(range.lower(), startRow, 1);
                ByteArrays.writeLong(range.upper(), endRow, 1);
                if (range.contained()) {
                    startRow[9] = 0;
                    endRow[9] = 1;
                } else {
                    startRow[9] = ranges._2().byteValue();
                    endRow[9] = (byte) (ranges._3().byteValue() + 1L);
                    //System.out.println(range + "," + i + "," + ranges._2());
                }

                Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
                scan.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL));
                scan.setCaching(200);
                //scan.setFilter(new SpatialFilter(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_CF), WKTUtils.read(mbr.toText())));
                ResultScanner resultScanner = null;
                try {
                    resultScanner = hTable.getScanner(scan);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                for (Result res : resultScanner) {
                    if (range.contained()) {
                        resultList.add(res);
                        size.addAndGet(1);
                    } else {
                        Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL))));
                        if (geo.intersects(mbr)) {
                            resultList.add(res);
                            size.addAndGet(1);
                        }
                        //resultList.add(res);
                    }
                    totalSize.addAndGet(1);
                }
                resultScanner.close();
            }
        }
        if (printLogs) {
            System.out.println(String.format("%s,%s,%s,%s,%s", indexTime, System.currentTimeMillis() - time, totalSize.get(), size.get(), rangeSize.get()));
        }
        return resultList;
    }

    @Override
    public List<Result> rangeQuery(Double minLng, Double minLat, Double maxLng, Double maxLat) throws
            IOException, InterruptedException {
        long time = System.currentTimeMillis();
        Iterator<Iterable<IndexRange>> rs = this.sfc.ranges(minLng, minLat, maxLng, maxLat).sliding(10, 10);
        //Iterator<IndexRange> rss = this.sfc.ranges(minLng, minLat, maxLng, maxLat).toIterator();
        //System.out.println("range size:"+rss.size());
        Geometry mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat).toPolygon(4326);
        final List<Result> resultList = new ArrayList<>();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("demo-pool-%d").build();
        long indexTime = System.currentTimeMillis() - time;
        ExecutorService singleThreadPool = new ThreadPoolExecutor(10, 20,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        AtomicInteger size = new AtomicInteger(0);
        AtomicInteger totalSize = new AtomicInteger(0);
        AtomicInteger rangeSize = new AtomicInteger(0);

        while (rs.hasNext()) {
            Iterator<IndexRange> ranges = rs.next().toIterator();
            singleThreadPool.execute(() -> {
                while (ranges.hasNext()) {
                    try (HTable table = new HTable(TableName.valueOf(tableName), connection)) {
                        IndexRange range = ranges.next();
                        rangeSize.addAndGet(1);
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
                                synchronized (resultList) {
                                    if (!range.contained()) {
                                        Geometry geo = WKTUtils.read(Bytes.toString(res.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL))));
                                        if (geo.intersects(mbr)) {
                                            resultList.add(res);
                                            size.addAndGet(1);
                                        }
                                    } else {
                                        resultList.add(res);
                                        size.addAndGet(1);
                                    }
                                    totalSize.addAndGet(1);
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
        singleThreadPool.shutdown();
        singleThreadPool.awaitTermination(3, TimeUnit.SECONDS);
        if (printLogs) {
            System.out.println(String.format("%s,%s,%s,%s,%s", indexTime, System.currentTimeMillis() - time, totalSize.get(), size.get(), rangeSize.get()));
        }
//        System.out.println(System.currentTimeMillis() - time);
//        System.out.println("query size:" + totalSize.get());
//        System.out.println("size:" + size.get());
//        System.out.println("range size:" + rangeSize.get());
        return resultList;
    }

    public List<Result> MuiltRangeQuery(Double minLng, Double minLat, Double maxLng, Double maxLat) throws
            IOException, InterruptedException {
        Iterator<IndexRange> xzRanges = this.sfc.ranges(minLng, minLat, maxLng, maxLat).toIterator();
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

        while (xzRanges.hasNext()) {
            long t1 = System.currentTimeMillis();
            IndexRange range = xzRanges.next();
            byte[] startRow = new byte[9];
            byte[] endRow = new byte[9];
            singleThreadPool.execute(() -> {
                for (int i = 0; i < 4; i++) {
                    startRow[0] = (byte) i;
                    endRow[0] = (byte) i;
                    ByteArrays.writeLong(range.lower(), startRow, 1);
                    ByteArrays.writeLong(range.upper(), endRow, 1);
                    Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
                    scan.setCaching(200);
                    ResultScanner resultScanner = null;
                    try {
                        resultScanner = hTable.getScanner(scan);
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
                    }
                }
            });
        }
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
