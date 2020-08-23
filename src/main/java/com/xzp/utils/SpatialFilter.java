package com.xzp.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2020-08-21 15:23
 * @modified by :
 **/
public class SpatialFilter extends FilterBase {
    protected byte[] columnFamily;
    protected byte[] columnQualifier;
    protected Geometry value;
    protected boolean foundColumn = false;
    protected boolean matchedColumn = false;
    protected boolean filterIfMissing = false;
    protected boolean latestVersionOnly = true;

    public SpatialFilter(byte[] columnFamily, byte[] columnQualifier, Geometry value) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.value = value;
    }

    @Override
    public ReturnCode filterKeyValue(Cell c) throws IOException {
        if (this.matchedColumn) {
            // We already found and matched the single column, all keys now pass
            return ReturnCode.INCLUDE;
        } else if (this.latestVersionOnly && this.foundColumn) {
            // We found but did not match the single column, skip to next row
            return ReturnCode.NEXT_ROW;
        }
        if (!CellUtil.matchingColumn(c, this.columnFamily, this.columnQualifier)) {
            return ReturnCode.INCLUDE;
        }
        foundColumn = true;
        //c.getValue()
        if (filterColumnValue(c.getValue())) {
            return this.latestVersionOnly ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
        }
        this.matchedColumn = true;
        return ReturnCode.INCLUDE;
    }

    private boolean filterColumnValue(final byte[] data) {
        String geom = Bytes.toString(data);
        Geometry geometry = WKTUtils.read(geom);
        return !value.intersects(geometry);
    }

    @Override
    public Cell transformCell(Cell v) {
        return v;
    }

    @Override
    public boolean filterRow() {
        // If column was found, return false if it was matched, true if it was not
        // If column not found, return true if we filter if missing, false if not
        return this.foundColumn ? !this.matchedColumn : this.filterIfMissing;
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void reset() {
        foundColumn = false;
        matchedColumn = false;
    }
    @Override
    public boolean isFamilyEssential(byte[] name) {
        return !this.filterIfMissing || Bytes.equals(name, this.columnFamily);
    }
}
