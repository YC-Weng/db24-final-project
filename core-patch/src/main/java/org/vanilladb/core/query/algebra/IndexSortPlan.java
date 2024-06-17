package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.IVF.IVFIndex;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexSortPlan implements Plan {

    private Plan p, dp;
    private Transaction tx;
    private DistanceFn distFn;
    private IndexInfo ii;

    public IndexSortPlan(Plan p, DistanceFn distFn, IndexInfo ii, Transaction tx) {
        this.p = p;
        this.distFn = distFn;
        this.tx = tx;
        this.ii = ii;
    }

    @Override
    public Scan open() {
        return new IndexSortScan(this.p.open(), distFn);
    }

    @Override
    public long blocksAccessed() {
        return this.p.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return p.schema();
    }

    @Override
    public Histogram histogram() {
        return p.histogram();
    }

    @Override
    public long recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public String toString() {
        String c = p.toString();
        String[] cs = c.split("\n");
        StringBuilder sb = new StringBuilder();
        sb.append("->");
        sb.append("IndexSortPlan (#blks=" + blocksAccessed() + ", #recs="
                + recordsOutput() + ")\n");
        for (String child : cs)
            sb.append("\t").append(child).append("\n");
        ;
        return sb.toString();
    }
}
