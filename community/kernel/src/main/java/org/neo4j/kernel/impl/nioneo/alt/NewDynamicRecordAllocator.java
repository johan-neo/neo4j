package org.neo4j.kernel.impl.nioneo.alt;

import java.util.Iterator;

import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecordAllocator;

public class NewDynamicRecordAllocator implements DynamicRecordAllocator
{
    private final Store store;
    
    public NewDynamicRecordAllocator( Store store )
    {
        this.store = store;
    }
   
    @Override
    public DynamicRecord nextUsedRecordOrNew( Iterator<DynamicRecord> recordsToUseFirst )
    {
        DynamicRecord record;
        if ( recordsToUseFirst.hasNext() )
        {
            record = recordsToUseFirst.next();
            if ( !record.inUse() )
            {
                record.setCreated();
            }
        }
        else
        {
            record = new DynamicRecord( store.getIdGenerator().nextId() );
            record.setCreated();
        }
        record.setInUse( true );
        return record;
    }

    @Override
    public int dataSize()
    {
        return store.getRecordStore().getRecordSize() - AbstractDynamicStore.BLOCK_HEADER_SIZE;
    }
}
