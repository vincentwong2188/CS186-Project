package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for(Lock lock : this.locks) {
                if (lock.transactionNum == except) { continue; }
                if (LockType.compatible(lockType, lock.lockType)) { return true; }
            }
            return false;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            Long transactionNum = lock.transactionNum;
            // grant
            if (!transactionLocks.containsKey(transactionNum)) {
                List<Lock> locksList = new ArrayList<>();
                locksList.add(lock);
                transactionLocks.put(transactionNum, locksList);
                this.locks.add(lock);
            } else { // update
                List<Lock> locksList = new ArrayList<>();
                locksList.add(lock);
                transactionLocks.replace(transactionNum, locksList);
                for (Lock l : this.locks) {
                    if (l.name == lock.name) {
                        this.locks.remove(l);
                        continue;
                    }
                this.locks.add(lock);
                }
            }
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            this.locks.remove(lock);
            List<Lock> transLocks = transactionLocks.getOrDefault(lock, new ArrayList<Lock>());
            transLocks.remove(lock);
            if (transLocks.isEmpty()) {
                transactionLocks.remove(lock.transactionNum);
            } else {
                transactionLocks.put(lock.transactionNum, transLocks);
            }
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) { this.waitingQueue.addFirst(request); }
            else { this.waitingQueue.addLast(request); }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            /*
            Iterator iter = this.waitingQueue.iterator();
            while(iter.hasNext()) {
                LockRequest lockRequest = iter.next();
                // ? what is except
                if (!this.checkCompatible(lockRequest.lock.lockType, 0)) { return; }

            }
            return;
            */
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for(Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(proj4_part1): You may add helper methods here if you wish

    public Lock conflictingLock(Lock lock) {
        ResourceName name = lock.name;
        long transactionNum = lock.transactionNum;
        ResourceEntry resourceEntry = this.getResourceEntry(name);
        List<Lock> locksList = resourceEntry.locks;
        // resource not holding any locks (granted set is empty)
        if (locksList.isEmpty()) { return null; }
        // waiting queue of resource is empty 
        if (!resourceEntry.waitingQueue.isEmpty()) { return null; }
        
        for (Lock l : locksList) {
            // if there is a conflict
            if (!LockType.compatible(l.lockType, lock.lockType)) { return l; }
        }

        return null;
    }

    

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            this.acquire(transaction, name, lockType);
            if (transaction.getBlocked()) {
                try {
                    promote(transaction, name, lockType);
                } 
                catch (DuplicateLockRequestException | NoLockHeldException | InvalidLockException e) { return; }
                transaction.unblock();
            } else {
                for (ResourceName n : releaseLocks) {
                    release(transaction, n);
                }
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // public Lock(ResourceName name, LockType lockType, long transactionNum)
            ResourceEntry resourceEntry = this.getResourceEntry(name);
            Long transactionNum = transaction.getTransNum();
            Lock lock = new Lock(name, lockType, transactionNum);
            // if queue is empty
            if (resourceEntry.waitingQueue.isEmpty()) {
                resourceEntry.grantOrUpdateLock(lock);
            } else { // add to waiting queue and block transaction 
                Lock confLock = this.conflictingLock(lock);
                // if the lock already exists in waiting queue
                if (confLock != null) {
                    if (confLock.transactionNum == transactionNum && confLock.lockType == lockType) {
                        throw new DuplicateLockRequestException("a lock on NAME is held by TRANSACTION");
                    }
                } else {
                    resourceEntry.waitingQueue.add(new LockRequest(transaction, lock));
                    shouldBlock = true;
                }
            }
        }
        
        if (shouldBlock) { transaction.block(); }

    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            // if no lock on NAME is held by TRANSACTION
            if (getLockType(transaction, name) == LockType.NL) {
                throw new NoLockHeldException("no lock on NAME is held by TRANSACTION");
            }
            // public Lock(ResourceName name, LockType lockType, long transactionNum)
            ResourceEntry resourceEntry = this.getResourceEntry(name);
            Long transactionNum = transaction.getTransNum();
            List<Lock> locksList = this.getLocks(name);
            Lock lock = null;
            for (Lock l : locksList) {
                if (l.transactionNum == transactionNum) {
                    lock = l;
                }
            }
            
            // if specific lock is found on transaction
            if (lock != null) {
                resourceEntry.releaseLock(lock);
                if (!resourceEntry.waitingQueue.isEmpty()) {
                    LockRequest firstLockRequest = resourceEntry.waitingQueue.getFirst();
                    Lock firstLock = firstLockRequest.lock;
                    Lock firstLockConfLock = this.conflictingLock(firstLock);

                    //!
                }

            }
            
            
        }
        transaction.unblock();
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            // public Lock(ResourceName name, LockType lockType, long transactionNum)
            ResourceEntry resourceEntry = this.getResourceEntry(name);
            Long transactionNum = transaction.getTransNum();
            List<Lock> locksList = this.getLocks(name);
            Lock lock = null;
            for (Lock l : locksList) {
                if (l.transactionNum == transactionNum) {
                    lock = l;
                }
            }
    
            // if TRANSACTION already has a NEWLOCKTYPE lock on NAME
            if (lock.lockType == newLockType) {
                throw new DuplicateLockRequestException("TRANSACTION already has a NEWLOCKTYPE lock on NAME");
            }

            // if TRANSACTION has no lock on NAME
            if (lock == null) {
                throw new NoLockHeldException("TRANSACTION has no lock on NAME");
            }
            
            // same lock, except with newLockType
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            // if the requested lock type is not a promotion
            if (!LockType.substitutable(newLockType, lock.lockType)) {
                throw new InvalidLockException("the requested lock type is not a promotion");
            }
            
            // remove old lock
            locksList.remove(lock);
            // check for existence of conflicting lock
            if (this.conflictingLock(newLock) == null) {
                // no conflicting lock, promote and add back
                lock.lockType = newLockType;
                locksList.add(lock);
            } else {
                // add back removed original lock, create new lock request and add to waiting queue
                locksList.add(lock);
                List<Lock> lockRequList = new ArrayList<>();
                lockRequList.add(lock);
                // LockRequest(TransactionContext transaction, Lock lock, List<Lock> releasedLocks)
                LockRequest request = new LockRequest(transaction, newLock, lockRequList);
                resourceEntry.addToQueue(request, true);
            }
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        List<Lock> locksList = this.getLocks(name);
        if (locksList.isEmpty()) {
            return LockType.NL;
        } else {
            for (Lock lock : locksList) {
                if (lock.transactionNum == transaction.getTransNum()) {
                    return lock.lockType;
                }
            }
        }
        // if there are no matches
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
