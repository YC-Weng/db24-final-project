package org.vanilladb.core.storage.index.IVF;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

public class IVFIndex extends Index {

    /**
     * A field name of the schema of index records.
     */
    private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
            SCHEMA_RID_ID = "id";
    private static Logger logger = Logger.getLogger(IVFIndex.class.getName());

    public static final int NUM_DIMENSION, NUM_CENTROIDS, MAX_TRAINING_ITER, DIMENSION_DATA_UPPER_BOUND,
            MAX_TRAINING_TIME;

    static {
        NUM_DIMENSION = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".NUM_DIMENSIONS", 48);
        NUM_CENTROIDS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".NUM_CENTROIDS", 20);
        MAX_TRAINING_ITER = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".MAX_TRAINING_ITER", 20);
        DIMENSION_DATA_UPPER_BOUND = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".DIMENSION_DATA_UPPER_BOUND", 218);
        MAX_TRAINING_TIME = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".MAX_TRAINING_TIME", 1200);

    }

    // public static long searchCost(SearchKeyType keyType, long totRecs, long
    // matchRecs) {
    // int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
    // return (totRecs / rpb) / NUM_BUCKETS;
    // }

    private static String keyFieldName(int index) {
        return SCHEMA_KEY + index;
    }

    /**
     * Returns the schema of the index records.
     * 
     * @param fldType
     *                the type of the indexed field
     * 
     * @return the schema of the index records
     */
    private static Schema schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        for (int i = 0; i < keyType.length(); i++)
            sch.addField(keyFieldName(i), keyType.get(i));
        sch.addField("centroid_num", INTEGER);
        return sch;
    }

    private static Schema data_schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        for (int i = 0; i < keyType.length(); i++)
            sch.addField(keyFieldName(i), keyType.get(i));
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }

    // private static Schema temp_data_schema(SearchKeyType keyType) {
    // Schema sch = new Schema();
    // for (int i = 0; i < keyType.length(); i++)
    // sch.addField(keyFieldName(i), keyType.get(i));
    // sch.addField(SCHEMA_RID_BLOCK, BIGINT);
    // sch.addField(SCHEMA_RID_ID, INTEGER);
    // sch.addField("centroid_num", INTEGER);
    // return sch;
    // }

    private class DpBlkRid {
        public Constant vc, blk, rid;

        DpBlkRid(Constant vc, Constant blk, Constant rid) {
            this.vc = vc;
            this.blk = blk;
            this.rid = rid;
        }
    }

    private SearchKey searchKey;
    private RecordFile rf;
    private boolean isBeforeFirsted;
    private Map<IntegerConstant, Constant> centroidMap;
    private Map<Constant, List<DpBlkRid>> centDpMap;
    private long startTrainTime;
    private int minDataNum;

    /**
     * Opens a hash index for the specified index.
     * 
     * @param ii
     *                the information of this index
     * @param keyType
     *                the type of the search key
     * @param tx
     *                the calling transaction
     */
    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
    }

    // create centroid file and temp data file
    @Override
    public void Initialization() {
        close();

        TableInfo ti = new TableInfo(ii.indexName() + "_centroid", schema(keyType));
        rf = ti.open(tx, false);
        RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.close();

        tx.bufferMgr().flushAll();
    }

    private void prepare_for_training() {
        close();

        centDpMap = new HashMap<Constant, List<DpBlkRid>>();
        centroidMap = new HashMap<IntegerConstant, Constant>();

        for (int i = 0; i < NUM_CENTROIDS; i++)
            centDpMap.put(new IntegerConstant(i), new LinkedList<DpBlkRid>());

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            TableInfo ti = new TableInfo(ii.indexName() + "_data_" + String.valueOf(i), data_schema(keyType));
            rf = ti.open(tx, false);
            rf.beforeFirst();
            while (rf.next()) {
                if (!centDpMap.keySet().contains(new IntegerConstant(i)))
                    centDpMap.put(new IntegerConstant(i), new LinkedList<DpBlkRid>());

                centDpMap.get(new IntegerConstant(i)).add(new DpBlkRid(rf.getVal(keyFieldName(0)),
                        rf.getVal(SCHEMA_RID_BLOCK), rf.getVal(SCHEMA_RID_ID)));
            }
            rf.close();
        }

        minDataNum = 1000000;
        for (int i = 0; i < NUM_CENTROIDS; i++)
            if (centDpMap.get(new IntegerConstant(i)).size() < minDataNum
                    && centDpMap.get(new IntegerConstant(i)).size() != 0)
                minDataNum = centDpMap.get(new IntegerConstant(i)).size();
    }

    // using kmeans to train the index
    public void TrainIndex(long timeTaken) {
        close();

        startTrainTime = System.currentTimeMillis();

        prepare_for_training();

        long prevTime = startTrainTime;
        int i = 1;
        while (i < MAX_TRAINING_ITER) {
            Map<Constant, List<DpBlkRid>> oldCentDpMap = new HashMap<Constant, List<DpBlkRid>>(centDpMap);

            calculate_new_centroids();
            reassign_all_the_data(oldCentDpMap);

            logger.info("After iteration " + String.valueOf(i) + ": \n" + print_cent_data_num_info(oldCentDpMap)
                    + "iteration " + String.valueOf(i) + ": "
                    + String.valueOf((System.currentTimeMillis() - prevTime) / 1000.0) + " seconds\n"
                    + "total elapsed time: "
                    + String.valueOf((System.currentTimeMillis() - startTrainTime + timeTaken) / 1000.0)
                    + " seconds\n");

            // if the training converge then stop
            if ((any_change(oldCentDpMap) == false && i > 10)
                    || (System.currentTimeMillis() - startTrainTime > MAX_TRAINING_TIME * 1000))
                break;
            prevTime = System.currentTimeMillis();
            i++;
        }

        // write the trained centroids and data back to their files
        write_back_new_centroids();
        write_back_new_data();
    }

    private void calculate_new_centroids() {
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            Constant sum = VectorConstant.zeros(NUM_DIMENSION);
            for (DpBlkRid dp : centDpMap.get(new IntegerConstant(i)))
                sum = sum.add(dp.vc);
            if (centDpMap.get(new IntegerConstant(i)).size() > 20)
                centroidMap.put(new IntegerConstant(i),
                        sum.div(new IntegerConstant(centDpMap.get(new IntegerConstant(i)).size())));
            else
                centroidMap.put(new IntegerConstant(i), steal_some_data_to_gen_vec());
        }
    }

    private VectorConstant steal_some_data_to_gen_vec() {
        Random rvg = new Random();
        List<DpBlkRid> select_list = centDpMap.get(new IntegerConstant(rvg.nextInt(NUM_CENTROIDS)));
        while (select_list.size() == 0)
            select_list = centDpMap.get(new IntegerConstant(rvg.nextInt(NUM_CENTROIDS)));
        return (VectorConstant) select_list.get(rvg.nextInt(select_list.size())).vc;
    }

    private void reassign_all_the_data(Map<Constant, List<DpBlkRid>> oldCentDpMap) {
        centDpMap = new HashMap<Constant, List<DpBlkRid>>();
        for (int i = 0; i < NUM_CENTROIDS; i++)
            centDpMap.put(new IntegerConstant(i), new LinkedList<DpBlkRid>());
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            for (DpBlkRid dp : oldCentDpMap.get(new IntegerConstant(i))) {
                int nearest_cent = calc_nearest_cent_num((VectorConstant) dp.vc);
                centDpMap.get(new IntegerConstant(nearest_cent)).add(dp);
            }
        }
    }

    private boolean any_change(Map<Constant, List<DpBlkRid>> oldCentDpMap) {
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            if (oldCentDpMap.get(new IntegerConstant(i)).size() != centDpMap.get(new IntegerConstant(i)).size())
                return true;
        }
        return false;
    }

    private void write_back_new_centroids() {
        close();
        TableInfo ti = new TableInfo(ii.indexName() + "_centroid", schema(keyType));
        rf = ti.open(tx, false);

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            rf.insert();
            rf.setVal(keyFieldName(0), centroidMap.get(new IntegerConstant(i)));
            rf.setVal("centroid_num", new IntegerConstant(i));
        }
        rf.close();
        tx.bufferMgr().flushAll();
    }

    private void write_back_new_data() {
        close();

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            TableInfo ti = new TableInfo(ii.indexName() + "_data_" + String.valueOf(i), data_schema(keyType));
            RecordFile datarf = ti.open(tx, false);
            datarf.remove();

            datarf = ti.open(tx, false);
            RecordFile.formatFileHeader(ti.fileName(), tx);
            for (DpBlkRid dp : centDpMap.get(new IntegerConstant(i))) {
                datarf.insert();
                datarf.setVal(keyFieldName(0), dp.vc);
                datarf.setVal(SCHEMA_RID_BLOCK, dp.blk);
                datarf.setVal(SCHEMA_RID_ID, dp.rid);
            }
            datarf.close();
        }
        tx.bufferMgr().flushAll();
    }

    private String print_cent_data_num_info(Map<Constant, List<DpBlkRid>> oldCentDpMap) {
        String s = "";
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            s = s + "Centroid " + String.valueOf(i) + ": "
                    + String.valueOf(
                            oldCentDpMap.get(new IntegerConstant(i)).size())
                    + " -> "
                    + String.valueOf(centDpMap.get(new IntegerConstant(i)).size()) + "\n";
        }
        return s;
    }

    @Override
    public void preLoadToMemory() {
        String tblname = ii.indexName() + "_centroid.tbl";
        long blk_size = fileSize(tblname);
        BlockId blk;
        for (int j = 0; j < blk_size; j++) {
            blk = new BlockId(tblname, j);
            tx.bufferMgr().pin(blk);
        }
    }

    /**
     * Positions the index before the first index record having the specified
     * search key. The method hashes the search key to determine the bucket, and
     * then opens a {@link RecordFile} on the file corresponding to the bucket.
     * The record file for the previous bucket (if any) is closed.
     * 
     * @see Index#beforeFirst(SearchRange)
     */
    @Override
    public void beforeFirst(SearchRange searchRange) {

    }

    public void beforeFirst(SearchKey searchKey) {
        close();

        this.searchKey = searchKey;
        load_all_the_centroids();
        TableInfo ti = new TableInfo(ii.indexName() + "_data_" + String.valueOf(calc_nearest_cent_num(searchKey)),
                data_schema(keyType));
        rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();

        isBeforeFirsted = true;
    }

    private int calc_nearest_cent_num(SearchKey searchKey) {
        int smallestCentroidNum = 0;
        float minDistance = Float.MAX_VALUE;
        VectorConstant queryVector = (VectorConstant) searchKey.get(0);

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            VectorConstant centroid = (VectorConstant) centroidMap.get(new IntegerConstant(i));
            float distance = SIMDOperations.simdEuclideanDistance(queryVector.asJavaVal(), centroid.asJavaVal());
            if (distance < minDistance) {
                minDistance = distance;
                smallestCentroidNum = i;
            }
        }
        return smallestCentroidNum;
    }

    private int calc_nearest_cent_num(VectorConstant vc) {
        int smallestCentroidNum = 0;
        double minDistance = Float.MAX_VALUE;
        float[] queryVector = vc.asJavaVal();

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            VectorConstant centroid = (VectorConstant) centroidMap.get(new IntegerConstant(i));
            float[] centroidVector = centroid.asJavaVal();
            float distance = SIMDOperations.simdEuclideanDistance(queryVector, centroidVector);

            if (distance < minDistance) {
                minDistance = distance;
                smallestCentroidNum = i;
            }
        }
        return smallestCentroidNum;
    }

    private void load_all_the_centroids() {
        close();
        centroidMap = new HashMap<IntegerConstant, Constant>();
        TableInfo ti = new TableInfo(ii.indexName() + "_centroid", schema(keyType));
        rf = ti.open(tx, false);
        rf.beforeFirst();
        while (rf.next())
            centroidMap.put((IntegerConstant) rf.getVal("centroid_num"), rf.getVal(keyFieldName(0)));
        rf.close();
    }

    /**
     * Moves to the next index record having the search key.
     * 
     * @see Index#next()
     */
    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '"
                    + ii.indexName() + "'");

        while (rf.next())
            if (getKey().equals(searchKey))
                return true;
        return false;
    }

    /**
     * Retrieves the data record ID from the current index record.
     * 
     * @see Index#getDataRecordId()
     */
    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    /**
     * Inserts a new index record into this index.
     * 
     * @see Index#insert(SearchKey, RecordId, boolean)
     */
    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        // search the position
        beforeFirst(key);

        // insert the data
        rf.insert();

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        for (int i = 0; i < keyType.length(); i++)
            rf.setVal(keyFieldName(i), key.get(i));
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
                .number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
        rf.close();
    }

    public void insert_random(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        // random choose a centroid data file
        close();

        Random rvg = new Random();
        TableInfo ti = new TableInfo(ii.indexName() + "_data_" + String.valueOf(rvg.nextInt(NUM_CENTROIDS)),
                data_schema(keyType));
        rf = ti.open(tx, false);

        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);

        rf.insert();
        rf.setVal(keyFieldName(0), key.get(0));
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
                .number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
        rf.close();
    }

    /**
     * Deletes the specified index record.
     * 
     * @see Index#delete(SearchKey, RecordId, boolean)
     */
    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {

    }

    /**
     * Closes the index by closing the current table scan.
     * 
     * @see Index#close()
     */
    @Override
    public void close() {
        if (rf != null)
            rf.close();
    }

    private long fileSize(String fileName) {
        tx.concurrencyMgr().readFile(fileName);
        return VanillaDb.fileMgr().size(fileName);
    }

    private SearchKey getKey() {
        Constant[] vals = new Constant[keyType.length()];
        for (int i = 0; i < vals.length; i++)
            vals[i] = rf.getVal(keyFieldName(i));
        return new SearchKey(vals);
    }

    public TableInfo getCentroidTableInfo() {
        return new TableInfo(ii.indexName() + "_centroid", schema(keyType));
    }

    public TableInfo getDataTableInfo(int i) {
        return new TableInfo(ii.indexName() + "_data_" + String.valueOf(i), data_schema(keyType));
    }
}
