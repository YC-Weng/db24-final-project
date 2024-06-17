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
import org.vanilladb.core.storage.index.IVF.SIMDOperations;
import org.vanilladb.core.storage.record.RecordId;

public class IndexSortScan implements Scan {

    private Scan s, ds;
    private DistanceFn distFn;
    private Map<Double, Map<Constant, Constant>> distBlkRidMap;
    private Iterator<Entry<Double, Map<Constant, Constant>>> distBlkRidIter;
    private Map<Float, Constant> distIidMap;
    private Iterator<Entry<Float, Constant>> distIidIter;
    private Entry<Float, Constant> cur_entry;

    public IndexSortScan(Scan s, Scan ds, DistanceFn distFn) {
        this.s = s;
        this.ds = ds;
        this.distFn = distFn;
    }

    public IndexSortScan(Scan s, DistanceFn distFn) {
        this.s = s;
        this.distFn = distFn;
    }

    @Override
    public void beforeFirst() {
        distIidMap = new TreeMap<Float, Constant>();
        float[] queryVector = distFn.getQueryVector().asJavaVal();
        s.beforeFirst();
        while (s.next()) {
            distIidMap.put((float) SIMDOperations
                    .simdEuclideanDistance(((VectorConstant) s.getVal("i_emb")).asJavaVal(), queryVector),
                    s.getVal("i_id"));
        }
        distIidIter = distIidMap.entrySet().iterator();
    }

    @Override
    public boolean next() {
        boolean hasNext = distIidIter.hasNext();
        if (hasNext == false)
            return false;
        cur_entry = distIidIter.next();
        return true;
    }

    @Override
    public void close() {
        s.close();
    }

    @Override
    public Constant getVal(String fldName) {
        return cur_entry.getValue();
    }

    @Override
    public boolean hasField(String fldName) {
        return s.hasField(fldName);
    }

}
