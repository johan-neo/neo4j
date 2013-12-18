package org.neo4j.kernel.impl.nioneo.alt;

public class DefaultSweeper implements Sweeper
{
    private double memCapacity;
    private HitStat prevStat; 
    
    public DefaultSweeper( HitStat prevStat, double memCapacity )
    {
        this.memCapacity = memCapacity;
        this.prevStat = prevStat;
    }

    public long getMemoryDelta( HitStat newStat )
    {
        double missFactor = 1.0d;
        double hitFactor = 1.0d;
        long missDiff = newStat.missCount - prevStat.missCount;
        long hitDiff = newStat.hitCount - prevStat.hitCount;
        if ( missDiff > 0 )
        {
            missFactor = 1.1d;
        }
        if ( hitDiff < 0 )
        {
            hitFactor = 1.03d;
        }
        if ( memCapacity > 1.0d )
        {
            return (long) (newStat.totalMemNeeded*hitFactor*missFactor);
        }
        if ( newStat.totalMemNeeded == 0 )
        {
            return (long) (memCapacity * missFactor * hitFactor * newStat.totalMemUsage * -1.5d);
        }
        return (long) (memCapacity * missFactor * hitFactor * newStat.totalMemUsage * -0.8d);        
    }
}
