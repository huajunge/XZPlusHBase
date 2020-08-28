package com.xzp.hbase;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.xzp.AbstractClient;
import com.xzp.geometry.MinimumBoundingBox;
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
import rx.Observable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-08-20 16:43
 * @modified by :
 **/
public class RtreeHBaseClient implements AbstractClient, Closeable {
    private Short precise;
    private Admin admin;
    private HTable hTable;
    private String tableName;
    private static String DEFAULT_CF = "cf";
    private static String DEFAULT_COL = "v";
    private static String DEFAULT_ID = "nid";
    private static Short shard = 4;
    private int BATCH_SIZE = 10000;
    private List<Put> putCache;
    private int putSize = 0;
    private RTree<String, Rectangle> rTree;

    public RtreeHBaseClient(String tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
        this.tableName = tableName;
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        if (!admin.tableExists(table.getTableName())) {
            create();
        }
        this.hTable = new HTable(TableName.valueOf(tableName), connection);
        this.rTree = RTree.create();
    }

    public RTree<String, Rectangle> getrTree() {
        return rTree;
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
        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_ID), Bytes.toBytes(id));
        this.rTree = this.rTree.add(id, Geometries.rectangleGeographic(bbox.getMinX(),
                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()));
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
        //Iterator<Iterable<IndexRange>> rs = this.sfc.ranges(minLng, minLat, maxLng, maxLat).sliding(10, 10);
        //System.out.println("range size:"+rss.size());
        long time = System.currentTimeMillis();
        MinimumBoundingBox mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat);
        Observable<Entry<String, Rectangle>> result = this.rTree.search(Geometries.rectangleGeographic(mbr.getMinX(),
                mbr.getMinY(), mbr.getMaxX(), mbr.getMaxY()));
        final List<Result> resultList = new ArrayList<>();
        int size = 0;
        int QueryBatch = 10000;
        List<Get> gets = new ArrayList<>();
        for (Entry<String, Rectangle> e : result.toBlocking().toIterable()) {
            gets.add(new Get(Bytes.toBytes(e.value())));
            size++;
        }
        long indexTime = (System.currentTimeMillis() - time);
        //time = System.currentTimeMillis();
        Result[] results = hTable.get(gets);
//        Polygon polygon = mbr.toPolygon(4326);
//        for (Result result1 : results) {
////            Geometry geo = WKTUtils.read(Bytes.toString(result1.getValue(Bytes.toBytes(DEFAULT_CF), Bytes.toBytes(DEFAULT_COL))));
////            if (geo.intersects(polygon)) {
////                resultList.add(result1);
////            }
//            resultList.add(result1);
//        }
        System.out.println(indexTime + "    " + (System.currentTimeMillis() - time) + "    " + size);
        return Arrays.asList(results);
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
    }
}
