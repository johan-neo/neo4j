package org.neo4j.kernel.impl.nioneo.alt;

public interface Sweeper
{
    public long getMemoryDelta( HitStat newStat );
}
