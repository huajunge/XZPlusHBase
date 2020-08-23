package com.xzp;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-08-20 16:56
 * @modified by :
 **/
public interface AbstractClient {
    /**
     * Create table
     *
     * @param
     * @return : void
     **/
    void create() throws IOException;

    /**
     * Insert data
     *
     * @param id
     * @param geom
     * @param value
     * @return : void
     **/
    void insert(String id, String geom, String value) throws IOException;

    /**
     * Range query
     *
     * @param minLng
     * @param minLat
     * @param maxLng
     * @param maxLat
     * @return : void
     **/
    List<Result> rangeQuery(Double minLng, Double minLat, Double maxLng, Double maxLat) throws IOException, InterruptedException;

    /**
     * update
     *
     * @param id
     * @param geom
     * @param value
     * @return : void
     **/
    void update(String id, String geom, String value);

    /**
     * delete
     *
     * @param id
     * @return : void
     **/
    void delete(String id);
}
