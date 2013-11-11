/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.alt;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;
import org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray;

/**
 * Dynamic store that stores strings.
 */
public class NeoLabelStore extends NeoDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";
    public static final String VERSION = NeoNeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
    public static final int DEFAULT_DATA_BLOCK_SIZE = 60;

    static long parseLabelsBody( long labelsField )
    {
        return labelsField & 0xFFFFFFFFFL;
    }

    private static boolean fieldPointsToDynamicRecordOfLabels( long labelField )
    {
        return (labelField & 0x8000000000L) != 0;
    }

    /**
     * @see NodeRecord
     *
     * @param labelField label field value from a node record
     * @return the id of the dynamic record this label field points to or null if it is an inline label field
     */
    public static Long fieldDynamicLabelRecordId( long labelField )
    {
        if ( fieldPointsToDynamicRecordOfLabels( labelField ) )
        {
            return parseLabelsBody( labelField );
        }
        else
        {
            return null;
        }
    }

    public static Collection<DynamicRecord> ensureHeavy( long labelField, RecordStore labelStore )
    {
        Long firstDynamicBlockId = NeoLabelStore.fieldDynamicLabelRecordId( labelField );
        if ( firstDynamicBlockId != null )
        {
            return NeoDynamicStore.getRecords( labelStore, firstDynamicBlockId, RecordLoad.NORMAL );
        }
        return Collections.<DynamicRecord>emptyList();
    }
    
    public static Collection<DynamicRecord> allocateRecordsForLabels( long nodeId, long[] labels,
            Collection<DynamicRecord> recordsToUseFirst, Store labelStore )
    {
        long[] storedLongs = LabelIdArray.prependNodeId( nodeId, labels );
        return NeoArrayStore.allocateFromNumbers( storedLongs, recordsToUseFirst, new NewDynamicRecordAllocator( labelStore ) ); 
    }
}
