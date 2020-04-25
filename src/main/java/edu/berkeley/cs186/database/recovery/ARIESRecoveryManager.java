package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.swing.event.AncestorEvent;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement

        // 1. Emits a Record: writing a log record to the log.
        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);
        long prevLSN = transactionTable.get(transNum).lastLSN;
        CommitTransactionLogRecord commitLogRecord = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN = this.logManager.appendToLog(commitLogRecord);

        // 2. Flushing the Log to the Log Disk Device
        this.logManager.flushToLSN(LSN);

        // 3. Updating transaction table and the transaction status
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        transactionEntry.lastLSN = LSN;

        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement

        System.out.println("In abort()");
        // get lastLSN of Xact from Xact table
        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);
        // update transaction status
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        
        // write an ABORT record to the log before starting to rollback operations
        long lastLSN = transactionEntry.lastLSN;
        AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transNum, lastLSN);
        this.logManager.appendToLog(abortTransactionLogRecord);
        Long abortLogLSN = abortTransactionLogRecord.getLSN();
        
        // set transaction's lastLSN to the LSN of this abort log record
        transactionEntry.lastLSN = abortLogLSN;
    
        return abortLogLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    // TestRecoveryManager.testAbortingEnd:957 expected:<{10000000001=70000}> but was:<{}>
    public long end(long transNum) {
        // TODO(proj5): implement
        // get lastLSN of Xact from Xact table
        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);
        long lastLSN = transactionEntry.lastLSN;
        EndTransactionLogRecord endTransactionLogRecord;

        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING) {

            abortOrRollback(lastLSN, transactionEntry, 0);
            this.transactionTable.remove(transNum);

        }

        // Find the LSN of the most recent CLR (if there is), i.e. the last entry
        endTransactionLogRecord = new EndTransactionLogRecord(transNum, transactionEntry.lastLSN);
        this.logManager.appendToLog(endTransactionLogRecord);

        // Update transaction status
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);

        this.transactionTable.remove(transNum);

        return endTransactionLogRecord.getLSN();
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(proj5): implement

        // Initialising some variables required
        long prevLSN = transactionTable.get(transNum).lastLSN;
        long LSNofLastRecord;

        if (after.length > BufferManager.EFFECTIVE_PAGE_SIZE / 2){
            // Two records should be emitted instead :
            // an undo-only record followed by a redo-only record

            UpdatePageLogRecord undoPageLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, null);
            long undoLSN = this.logManager.appendToLog(undoPageLogRecord);

            UpdatePageLogRecord redoPageLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, null, after);
            LSNofLastRecord = this.logManager.appendToLog(redoPageLogRecord);

        }else {
            // Just one Log Record is emitted
            UpdatePageLogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
            LSNofLastRecord = this.logManager.appendToLog(updatePageLogRecord);
        }

        // Updates Transaction Table
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        transactionEntry.lastLSN = LSNofLastRecord;
        transactionEntry.touchedPages.add(pageNum);

        // Updates Dirty Page Table (only if DPT doesn't already contain the pageID)
        if (!this.dirtyPageTable.containsKey(pageNum)) {
            this.dirtyPageTable.put(pageNum, LSNofLastRecord);
        }

        return LSNofLastRecord;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            System.out.println("enters here");
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages

        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement

        long lastLSN = transactionEntry.lastLSN;

        abortOrRollback(lastLSN, transactionEntry, savepointLSN);

        transactionEntry.lastLSN = savepointLSN;

        // Update transaction status
        transactionEntry.transaction.setStatus(Transaction.Status.RUNNING);

    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table

        // 1. Iterate through the dirtyPageTable and copy the entries. If at any point, copying the
        // current record would cause the end checkpoint record to be too large,
        // an end checkpoint record with the copied DPT entries should be appended to the log.
        for (Map.Entry<Long, Long> dirtyPage : this.dirtyPageTable.entrySet()) {
            long pageNum = dirtyPage.getKey();
            long recLSN = dirtyPage.getValue();
            boolean fitsAfterAdd3 = EndCheckpointLogRecord.fitsInOneRecord(dpt.size() + 1, txnTable.size(), touchedPages.size(), numTouchedPages);

            if (!fitsAfterAdd3){
                // Emit EndRecord into logManager since it no longer fits
                clearAllFields(dpt, txnTable, touchedPages, numTouchedPages);
            }

            // Copy the (pageNum, recLSN) tuple into the dpt HashMap
            dpt.put(pageNum, recLSN);
        }

        // 2. Iterate through the transaction table, and copy the status/lastLSN, outputting end checkpoint records only as needed.
        for (Map.Entry<Long, TransactionTableEntry> entry2 : transactionTable.entrySet()) {
            long transNum2 = entry2.getKey();
            long lastLSN = entry2.getValue().lastLSN;
            Transaction.Status status = entry2.getValue().transaction.getStatus();

            boolean fitsAfterAdd2 = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size() + 1, touchedPages.size(), numTouchedPages);

            if (!fitsAfterAdd2){
                // Emit EndRecord into logManager since it no longer fits
                clearAllFields(dpt, txnTable, touchedPages, numTouchedPages);

            }

            // Copy the (Transaction Number, (Status, lastLSN)) tuple into the txnTable HashMap
            Pair<Transaction.Status, Long> statusLSNPair = new Pair<>(status, lastLSN);
            txnTable.put(transNum2, statusLSNPair);
        }

        // 3. Iterate through the transaction table, and copy the touched pages,
        // outputting end checkpoint records only as needed.
        // Transactions without any touched pages should not appear here at all.

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    clearAllFields(dpt, txnTable, touchedPages, numTouchedPages);
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // 4. Output one final end checkpoint.
        LogRecord endRecord2 = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord2);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    private void clearAllFields(Map<Long, Long> dpt, Map<Long, Pair<Transaction.Status, Long>> txnTable, Map<Long, List<Long>> touchedPages, int numTouchedPages){
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        dpt.clear();
        txnTable.clear();
        touchedPages.clear();
        numTouchedPages = 0;
    }

    private void abortOrRollback(long lastLSN, TransactionTableEntry transactionEntry, long lowerLimit){
        while (lastLSN > lowerLimit) {
            // System.out.println("lastLSN: " + lastLSN);
            LogRecord logRecord = this.logManager.fetchLogRecord(lastLSN);
            // only records that are undoable should be undone
            if(logRecord.isUndoable()) {
                // last LSN for CLR, i.e. lastLSN of the transaction
                Pair<LogRecord, Boolean> p = logRecord.undo(transactionEntry.lastLSN);
                LogRecord clr = p.getFirst();
                long pageNum = clr.getPageNum().isPresent() ? clr.getPageNum().get() : -1L;
                long clrLSN = this.logManager.appendToLog(clr);


//                    // add edited page (if it exists) by CLR into DPT
//                    if (transactionEntry.touchedPages.contains(pageNum) && !this.dirtyPageTable.containsKey(pageNum) && pageNum > -1L) {
//                        this.dirtyPageTable.put(pageNum, clrLSN);
//                    }

                // Replaced the above commented out code with the following code:
                // Check for special dirty page table updates for UndoUpdatePageRecord and for UndoAllocPageRecord.
                // ie. We only update the dirty page table if we are undoing a page update
                // (add to DPT if not already there) or undoing a page allocation (remove from DPT if there)
                // Additional Info: some clr operations can potentially dirty pages (notably UndoUpdate),
                // so we have to add the page that it changes to the dirty page table as a result.
                // Similarly, the clr record UndoAllocPage should remove the corresponding page from the dirty page table
                // (as we are undoing the operation that allocated the page in the first place).
                // For every other type of clr record, they either don't operate on pages or just don't concern
                // the dirty page table; so in that case, we don't even need to worry about page num.
                if (clr instanceof UndoUpdatePageLogRecord) {
                    // add edited page (if it exists) by CLR into DPT
                    if (!this.dirtyPageTable.containsKey(pageNum) && pageNum > -1L) {
                        this.dirtyPageTable.put(pageNum, clrLSN);
                    }
                }else if (clr instanceof UndoAllocPageLogRecord || clr instanceof UndoFreePageLogRecord){
                    if (this.dirtyPageTable.containsKey(pageNum) && pageNum > -1L) {
                        this.dirtyPageTable.remove(pageNum);
                    }
                }

                // call redo on return CLR
                /** a boolean that is true
                 *  if the log must be flushed up to the CLR after executing the undo,
                 *  and false otherwise.
                 */
                if (p.getSecond()) {
                    this.logManager.flushToLSN(clr.getLSN());
                }

                clr.redo(diskSpaceManager, bufferManager);


                transactionEntry.lastLSN = clrLSN;
            }
            lastLSN = logRecord.getPrevLSN().get();


            // The following loop is to take into account Nested Rollbacks to save on redos
            // Logic: taking the latest record, getting its undoNextLSN, and then start rolling back from that
            // record on instead of rolling back on the already rolled-back LSNs by a previous parent nested rollback
            if (logRecord.getUndoNextLSN().isPresent()) {
                lastLSN = logRecord.getUndoNextLSN().get();
            }
        }
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): implement
        return () -> {};
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    /** Type of LogRecords
     * Transaction Operations:
     *      Partition-Related Log Records:
     *          AllocPartLogRecord
     *          FreePartLogRecord
     *          UndoAllocPartLogRecord
     *          UndoFreePartLogRecord
     *      Page-Related Log Records:
     *          Updates:
     *              UpdatePageLogRecord
     *              UndoUpdatePageLogRecord
     *          Non-Updates:
     *              AllocPageLogRecord
     *              FreePageLogRecord
     *              UndoAllocPageLogRecord
     *              UndoFreePageLogRecord
     * 
     * Transaction Status Changes:
     *      AbortTransactionLogRecord
     *      CommitTransactionLogRecord
     *      EndTransactionLogRecord
     * 
     * Checkpoint Record:
     *      BeginCheckpointLogRecord
     *      EndCheckpointLogRecord
     * Misc:
     *      MasterLogRecord
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start(begin) checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5): implement
        Iterator<LogRecord> iter = this.logManager.scanFrom(LSN);

        while (iter.hasNext()) {
            LogRecord logRecord = iter.next();
            /** If the log record is for a transaction operation:
            * - update the transaction table
            * - if it's page-related (as opposed to partition-related),
            *   - add to touchedPages
            *   - acquire X lock
            *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
            */
            // ? undoPart 
            if (logRecord.getClass().getName().contains("Part") || logRecord.getClass().getName().contains("Page")) {
                long transNum = logRecord.getTransNum().get();
                // If the transaction is not in the transaction table, it should be added to the table 
                // (the newTransaction function object can be used to create a Transaction object).
                if (!this.transactionTable.containsKey(transNum)) {
                    Transaction newXact = this.newTransaction.apply(transNum);
                    this.transactionTable.put(transNum, new TransactionTableEntry(newXact));
                }

                // The lastLSN of the transaction should be updated.
                this.transactionTable.get(transNum).lastLSN = logRecord.getLSN();

                // If the log record is about a page (as opposed to the partition-related log records), 
                // the page needs to be added to the touchedPages set in the transaction table entry 
                // and the transaction needs to request an X lock on it.
                if (logRecord.getClass().getName().contains("Page")) {
                    long pageNum = logRecord.getPageNum().get();
                    this.transactionTable.get(transNum).touchedPages.add(pageNum);

                    LockContext lockContext = this.getPageLockContext(pageNum);
                    this.acquireTransactionLock(this.transactionTable.get(transNum).transaction, lockContext, LockType.X);

                    // Updates: UpdatePage/UndoUpdatePage both may dirty a page in memory, without flushing changes to disk.
                    if (logRecord.getClass().getName().contains("Update")) {


                    }


                    // Non-Updates: AllocPage/FreePage/UndoAllocPage/UndoFreePage all make their changes visible on disk immediately, 
                    // and can be seen as flushing all changes at the time (including their own) to disk.
                    else {



                    }

                }



            }

            /** If the log record is for a change in transaction status:
            * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
            * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
            * - update the transaction table
            */
            if (logRecord.getClass().getName().contains("Transaction")) {
                long transNum = logRecord.getTransNum().get();
                if (logRecord.getClass().getName().contains("End")) {
                    this.transactionTable.get(transNum).transaction.cleanup();
                    this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                    this.transactionTable.remove(transNum);
                } 
                if (logRecord.getClass().getName().contains("Commit")) {
                    this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
                }
                if (logRecord.getClass().getName().contains("Abort")) {
                    this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                }
            }

            /** If the log record is a begin_checkpoint record:
            * - Update the transaction counter */
            if (logRecord.getClass().getName().contains("BeginCheckpoint")) {
                this.updateTransactionCounter.accept(logRecord.getMaxTransactionNum().get());
            }

            /** If the log record is an end_checkpoint record:
             * - Copy all entries of checkpoint DPT (replace existing entries if any)
             * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
             *   add to transaction table if not already present.
             * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
             *   transaction table if the transaction has not finished yet (i.e. !COMPELTE), and acquire X locks. */
            if (logRecord.getClass().getName().contains("EndCheckpoint")) {
                Map<Long, Long> checkpointDPT = logRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> checkpointXactTable = logRecord.getTransactionTable();
                Map<Long, List<Long>> touchedPagesMap = logRecord.getTransactionTouchedPages();

                for (Map.Entry<Long, Long> dirtyPage : checkpointDPT.entrySet()) {
                    long pageNum = dirtyPage.getKey();
                    long recLSN = dirtyPage.getValue();
        
                    this.dirtyPageTable.containsKey(pageNum) ? this.dirtyPageTable.replace(pageNum, recLSN) : this.dirtyPageTable.put(pageNum, recLSN);
                }

                for (Map.Entry<Long, Pair<Transaction.Status, Long>> xact : checkpointXactTable.entrySet()) { 
                    long transNum = xact.getKey();
                    long lastLSNinCheckPointRec = xact.getValue().getSecond();
                    long lastLSNinXactTable = this.transactionTable.get(transNum).lastLSN;

                    if (lastLSNinCheckPointRec > lastLSNinXactTable) {
                        this.transactionTable.get(transNum).lastLSN = lastLSNinCheckPointRec;
                    }
                }

                for (Map.Entry<Long, List<Long>> touchedPages : touchedPagesMap.entrySet()) {
                    long transNum = touchedPages.getKey();
                    List<Long> tps = touchedPages.getValue();
                    TransactionTableEntry xactEntry = this.transactionTable.get(transNum);

                    if (xactEntry.transaction.getStatus() != Transaction.Status.COMPLETE) {
                        for (Long tpNum : tps) {
                            LockContext lockContext = this.getPageLockContext(tpNum);
                            if (lockContext.getExplicitLockType(xactEntry.transaction.getTransactionContext()) == LockType.X) {
                                xactEntry.touchedPages.add(tpNum);
                            }
                        }

                    }

                }



            }
        }

        /** Then, cleanup and end transactions that are in the COMMITING state, and
        * move all transactions in the RUNNING state to RECOVERY_ABORTING. */
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            Transaction.Status status = entry.getValue().transaction.getStatus();
            long xactLastLSN = this.transactionTable.get(transNum).lastLSN;
            
            if (status == Transaction.Status.COMMITTING) {
                this.transactionTable.get(transNum).transaction.cleanup();
                this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(transNum, xactLastLSN);
                this.logManager.appendToLog(endTransactionLogRecord);
                this.transactionTable.remove(transNum);
                
            }
            
            if (status == Transaction.Status.RUNNING) {
                this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transNum, xactLastLSN);
                this.logManager.appendToLog(abortTransactionLogRecord);
            }

        }
        


    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5): implement
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        return;
    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////






    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
