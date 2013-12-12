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
import java.util.Collection;
import java.util.Collections;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

/**
 * Implementation of the node store.
 */
public class NeoNodeStore extends Store
{
    public static final String TYPE_DESCRIPTOR = "NodeStore";
    // in_use(byte)+next_rel_id(int)+next_prop_id(int)+labels(5)
    public static final int RECORD_SIZE = 14;
    
    public static final IdType ID_TYPE = IdType.NODE;

    public NeoNodeStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.NODE_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, false, RECORD_SIZE );
    }
    
    public static NodeRecord getRecord( long id, byte[] data )
    {
        return getRecord( id, data, RecordLoad.NORMAL );
    }
    
    public static NodeRecord getRecord( long id, byte[] data, RecordLoad load )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        // [    ,   x] in use bit
        // [    ,xxx ] higher bits for rel id
        // [xxxx,    ] higher bits for prop id
        long inUseByte = buffer.get();
        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            switch ( load )
            {
            case NORMAL:
                throw new InvalidRecordException( "NodeRecord[" + id + "] not in use" );
            case CHECK:
                return null;
            case FORCE:
                break;
            }
        }
        long nextRel = NeoNeoStore.unsginedInt( buffer.getInt() );
        long nextProp = NeoNeoStore.unsginedInt( buffer.getInt() );
        
        long relModifier = (inUseByte & 0xEL) << 31;
        long propModifier = (inUseByte & 0xF0L) << 28;
        
        long lsbLabels = NeoNeoStore.unsginedInt( buffer.getInt() );
        long hsbLabels = buffer.get() & 0xFF; // so that a negative bye won't fill the "extended" bits with ones.
        long labels = lsbLabels | (hsbLabels << 32);
        
        NodeRecord nodeRecord = new NodeRecord( id, NeoNeoStore.longFromIntAndMod( nextRel, relModifier ),
                NeoNeoStore.longFromIntAndMod( nextProp, propModifier ) );
        nodeRecord.setInUse( inUse );
        nodeRecord.setLabelField( labels );
        return nodeRecord;
    }

    public static byte[] updateRecord( NodeRecord record, byte[] data, boolean force )
    {
        // registerIdFromUpdateRecord( id );
        ByteBuffer buffer = ByteBuffer.wrap( data );
        if ( record.inUse() || force )
        {
            long nextRel = record.getNextRel();
            long nextProp = record.getNextProp();
            short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
                    : (short) ((nextRel & 0x700000000L) >> 31);
            short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0
                    : (short) ((nextProp & 0xF00000000L) >> 28);
            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = (record.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue();
            inUseUnsignedByte = (short) (inUseUnsignedByte | relModifier | propModifier);
            buffer.put( (byte) inUseUnsignedByte ).putInt( (int) nextRel ).putInt( (int) nextProp );
            // lsb of labels
            long labelField = record.getLabelField();
            buffer.putInt( (int) labelField );
            // msb of labels
            buffer.put( (byte) ((labelField & 0xFF00000000L) >>> 32) );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
        }
        return data;
    }
}
