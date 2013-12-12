/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
