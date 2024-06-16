package org.vanilladb.core.query.algebra;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Iterator;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;

public class IndexSortScan implements Scan {

    private Scan s, ds;
    private DistanceFn distFn;
    private Map<Double, Map<Constant, Constant>> distBlkRidMap;
    private Iterator<Entry<Double, Map<Constant, Constant>>> distBlkRidIter;

    public IndexSortScan(Scan s, Scan ds, DistanceFn distFn) {
        this.s = s;
        this.ds = ds;
        this.distFn = distFn;
    }

    @Override
    public void beforeFirst() {
        distBlkRidMap = new TreeMap<Double, Map<Constant, Constant>>();
        ds.beforeFirst();
        while (ds.next()) {
            Map<Constant, Constant> dataMap = new HashMap<Constant, Constant>();
            dataMap.put(ds.getVal("block"), ds.getVal("id"));
            distBlkRidMap.put(distFn.distance((VectorConstant) ds.getVal("key0")), dataMap);
        }
        distBlkRidIter = distBlkRidMap.entrySet().iterator();
    }

    @Override
    public boolean next() {
        boolean hasNext = distBlkRidIter.hasNext();
        if (hasNext == false)
            return false;
        Entry<Double, Map<Constant, Constant>> e = distBlkRidIter.next();
        Map<Constant, Constant> blkRid = e.getValue();
        for (Entry<Constant, Constant> ent : blkRid.entrySet()) {
            ((TableScan) this.s).moveToRecordId(
                    new RecordId(new BlockId(((TableScan) this.s).TblName(), (long) ent.getKey().asJavaVal()),
                            (int) ent.getValue().asJavaVal()));
        }
        return true;
    }

    @Override
    public void close() {
        s.close();
        ds.close();
    }

    @Override
    public Constant getVal(String fldName) {
        return s.getVal(fldName);
    }

    @Override
    public boolean hasField(String fldName) {
        return s.hasField(fldName);
    }

}
