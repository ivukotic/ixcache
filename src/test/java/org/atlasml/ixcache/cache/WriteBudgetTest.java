package org.atlasml.ixcache.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class WriteBudgetTest {

    @Test
    void refusesPastTheCapAndCountsWhatItRefused() {
        WriteBudget b = new WriteBudget(100);
        assertTrue(b.tryAcquire(60));
        assertTrue(b.tryAcquire(40));
        assertFalse(b.tryAcquire(1));
        b.release(40);
        assertFalse(b.tryAcquire(41));
        assertTrue(b.tryAcquire(40));
        assertEquals(42, b.takeSkipped());
        assertEquals(0, b.takeSkipped());
    }

    @Test
    void oneWriteLargerThanTheCapIsAllowedWhenNothingWaits() {
        WriteBudget b = new WriteBudget(100);
        assertTrue(b.tryAcquire(500));
        assertFalse(b.tryAcquire(1));
        b.release(500);
        assertTrue(b.tryAcquire(1));
    }
}
