package org.neo4j.kernel.impl.nioneo.alt;

public class HitStat
{
    public final long missCount;
    public final long hitCount;
    public final long totalMemUsage;
    public final long totalMemNeeded;
    
    public HitStat( long hitCount, long missCount, long totalMemUsage, long totalMemNeeded )
    {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.totalMemUsage = totalMemUsage;
        this.totalMemNeeded = totalMemNeeded;
    }
}
