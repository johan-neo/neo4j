package org.neo4j.kernel.impl.nioneo.alt;

import java.util.ArrayList;
import java.util.Collection;

public class SweeperThread extends Thread
{
    private volatile long availableMemory;
    private final ArrayList<RecordStore> recordStores = new ArrayList<RecordStore>();
    private volatile boolean doRun;
    
    public SweeperThread( long availableMemory, Collection<RecordStore> stores )
    {
        super( "RecordStorePageCacheSweeperThread" );
        recordStores.addAll( stores );
        this.availableMemory = availableMemory;
    }
    
    public void run()
    {
        final HitStat[] prevRound = new HitStat[recordStores.size()];
        while ( doRun )
        {
            long totalSize = 0;
            for ( RecordStore store : recordStores )
            {
                totalSize += store.getStoreSizeInBytes();
            }
            double memCapacity = availableMemory / (double) totalSize; 
            for ( int i = 0; i < prevRound.length; i++ )
            {
                HitStat prevStat = prevRound[i];
                RecordStore store = recordStores.get( i );
                prevRound[i] = store.sweep( new DefaultSweeper( prevStat, memCapacity ) );
            }
            try
            {
                sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }
    
    public void setAvailableMemory( long availableMemory )
    {
        this.availableMemory = availableMemory;
    }
    
    public long getAvailableMemorySet()
    {
        return availableMemory;
    }
    
    public void stopRunning()
    {
        doRun = false;
    }
}
