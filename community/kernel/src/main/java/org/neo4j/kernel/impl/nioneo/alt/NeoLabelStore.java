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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;
import org.neo4j.kernel.impl.nioneo.store.ShortArray;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.labels.LabelIdArray;
import org.neo4j.kernel.impl.util.Bits;

/**
 * Dynamic store that stores strings.
 */
public class NeoLabelStore extends Store
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";
    public static final String VERSION = NeoNeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
    public static final int DEFAULT_DATA_BLOCK_SIZE = 60;

    public static final IdType ID_TYPE = IdType.NODE_LABELS;
    
    public NeoLabelStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.NODE_LABELS_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, true, DEFAULT_DATA_BLOCK_SIZE );
    }

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
            return NeoDynamicStore.getRecords( labelStore, firstDynamicBlockId, RecordLoad.NORMAL, DynamicRecord.Type.STRING );
        }
        return Collections.<DynamicRecord>emptyList();
    }
    
    public static Collection<DynamicRecord> allocateRecordsForLabels( long nodeId, long[] labels,
            Collection<DynamicRecord> recordsToUseFirst, Store labelStore )
    {
        long[] storedLongs = LabelIdArray.prependNodeId( nodeId, labels );
        return NeoPropertyArrayStore.allocateFromNumbers( storedLongs, recordsToUseFirst, new NewDynamicRecordAllocator( labelStore, DynamicRecord.Type.STRING ) ); 
    }

    public static Pair<Long, long[]> getDynamicLabelsArrayAndOwner( Iterable<DynamicRecord> records )
    {
        long[] storedLongs = (long[])
                NeoPropertyArrayStore.getRightArray( NeoDynamicStore.readFullByteArray( records ) );
        return Pair.of(storedLongs[0], LabelIdArray.stripNodeId( storedLongs ));
    }
    
    public static Long readOwnerFromDynamicLabelsRecord( DynamicRecord record )
    {
        byte[] data = record.getData();
        
        byte[] header = readDynamicRecordHeader( data );

        int requiredBits = header[2];
        if ( requiredBits == 0 )
        {
            return null;
        }
        byte[] array = Arrays.copyOfRange( data, header.length, data.length );
        Bits bits = Bits.bitsFromBytes( array );
        return bits.getLong( requiredBits );
    }
    
    public static byte[] readDynamicRecordHeader( byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        byte typeId = buffer.get();
        if ( typeId == PropertyType.STRING.intValue() )
        {
            throw new IllegalStateException( "Header should indicate long array, not string array" );
        }
        byte[] header = new byte[3];
        System.arraycopy( data, 0, header, 0, Math.min( data.length, header.length) );
        return header;
    }
}
