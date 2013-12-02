package org.neo4j.kernel.impl.nioneo.alt;

import java.util.Iterator;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecordAllocator;

public class NewDynamicRecordAllocator implements DynamicRecordAllocator
{
    private final Store store;
    private final DynamicRecord.Type type;
    
    public NewDynamicRecordAllocator( Store store, DynamicRecord.Type type )
    {
        this.store = store;
        this.type = type;
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
            record = new DynamicRecord( store.getIdGenerator().nextId(), type );
            record.setCreated();
        }
        record.setInUse( true );
        return record;
    }

    @Override
    public int dataSize()
    {
        return store.getRecordStore().getRecordSize() - NeoDynamicStore.BLOCK_HEADER_SIZE;
    }
}
