package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    // refer to compatibility matrix
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if (a == NL || b == NL) {
            return true;
        }
        else if (a == IS) {
            if (b == X) { return false; }
            else { return true; }
        }
        else if (b == IS) {
            if (a == X) { return false; }
            else { return true; }
        }
        else if (a == IX) {
            if (b == IX) { return true; }
            else { return false; }
        }
        else if (b == IX) {
            if (a == IX) { return true; }
            else { return false; }
        }
        else if (a == S && b == S) { return true; }
        
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    // 1. To get S or IS lock on a node, must hold IS or IX on parent node
    // 2. To get X or IX on a node, must hold IX or SIX on parent node
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (childLockType == LockType.S || childLockType == LockType.IS) {
            if (parentLockType == LockType.IS || parentLockType == LockType.IX) { return true; }
        }
        if (childLockType == LockType.X || childLockType == LockType.IX) {
            if (parentLockType == LockType.IX || parentLockType == LockType.SIX) { return true; }
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    /**
     * S(A) can read A and all descendants of A.
     * X(A) can read and write A and all descendants of A.
     * IS(A) can request shared and intent-shared locks on all children of A.
     * IX(A) can request any lock on all children of A.
     * SIX(A) can do anything that having S(A) or IX(A) lets it do, except requesting S or IS locks on children of A, which would be redundant.
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // same
        if (substitute == required) { return true; }
        // required is no lock
        if (required == NL) { return true; }
        // S can be substituted for X or SIX
        if (required == S) {
            if (substitute == IX || substitute == SIX) { return true; }
        }
        // IS can be substituted for IX or SIX
        if (required == IS) {
            if (substitute == IX || substitute == SIX) { return true; }
        }
        // IX can be substituted for SIX or X
        if (required == IX) {
            if (substitute == SIX || substitute == X) { return true; }
        }
        
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

