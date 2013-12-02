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

import java.io.File;
import java.nio.ByteBuffer;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

/**
 * Implementation of the relationship store.
 */
public class NeoRelationshipStore extends Store
{
    
    public static final String TYPE_DESCRIPTOR = "RelationshipStore";

    // record header size
    // directed|in_use(byte)+first_node(int)+second_node(int)+rel_type(int)+
    // first_prev_rel_id(int)+first_next_rel_id+second_prev_rel_id(int)+
    // second_next_rel_id+next_prop_id(int)
    public static final int RECORD_SIZE = 33;

    public static final IdType ID_TYPE = IdType.RELATIONSHIP;
    
    public NeoRelationshipStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.RELATIONSHIP_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, false, RECORD_SIZE );
    }

    public static void updateRecord( RelationshipRecord record,
        byte[] data, boolean force )
    {
        // registerIdFromUpdateRecord( id );
        ByteBuffer buffer = ByteBuffer.wrap( data );
        if ( record.inUse() || force )
        {
            long firstNode = record.getFirstNode();
            short firstNodeMod = (short)((firstNode & 0x700000000L) >> 31);

            long secondNode = record.getSecondNode();
            long secondNodeMod = (secondNode & 0x700000000L) >> 4;

            long firstPrevRel = record.getFirstPrevRel();
            long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstPrevRel & 0x700000000L) >> 7;

            long firstNextRel = record.getFirstNextRel();
            long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (firstNextRel & 0x700000000L) >> 10;

            long secondPrevRel = record.getSecondPrevRel();
            long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondPrevRel & 0x700000000L) >> 13;

            long secondNextRel = record.getSecondNextRel();
            long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (secondNextRel & 0x700000000L) >> 16;

            long nextProp = record.getNextProp();
            long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & 0xF00000000L) >> 28;

            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            short inUseUnsignedByte = (short)((record.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue() | firstNodeMod | nextPropMod);

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            int typeInt = (int)(record.getType() | secondNodeMod | firstPrevRelMod | firstNextRelMod | secondPrevRelMod | secondNextRelMod);

            buffer.put( (byte)inUseUnsignedByte ).putInt( (int) firstNode ).putInt( (int) secondNode )
                .putInt( typeInt ).putInt( (int) firstPrevRel ).putInt( (int) firstNextRel )
                .putInt( (int) secondPrevRel ).putInt( (int) secondNextRel ).putInt( (int) nextProp );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
        }
    }

    public static RelationshipRecord getRecord( long id, byte[] data )
    {
        return getRecord( id, data, RecordLoad.NORMAL );
    }
    
    public static RelationshipRecord getRecord( long id, byte[] data,
        RecordLoad load )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );

        // [    ,   x] in use flag
        // [    ,xxx ] first node high order bits
        // [xxxx,    ] next prop high order bits
        long inUseByte = buffer.get();

        boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
        if ( !inUse )
        {
            switch ( load )
            {
            case NORMAL:
                throw new InvalidRecordException( "RelationshipRecord[" + id + "] not in use" );
            case CHECK:
                return null;
            default:
                break;
            }
        }

        long firstNode = NeoNeoStore.unsginedInt( buffer.getInt() );
        long firstNodeMod = (inUseByte & 0xEL) << 31;

        long secondNode = NeoNeoStore.unsginedInt( buffer.getInt() );

        // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
        // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
        // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
        // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
        // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
        // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
        long typeInt = buffer.getInt();
        long secondNodeMod = (typeInt & 0x70000000L) << 4;
        int type = (int)(typeInt & 0xFFFF);

        RelationshipRecord record = new RelationshipRecord( id,
            NeoNeoStore.longFromIntAndMod( firstNode, firstNodeMod ),
            NeoNeoStore.longFromIntAndMod( secondNode, secondNodeMod ), type );
        record.setInUse( inUse );

        long firstPrevRel = NeoNeoStore.unsginedInt( buffer.getInt() );
        long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
        record.setFirstPrevRel( NeoNeoStore.longFromIntAndMod( firstPrevRel, firstPrevRelMod ) );

        long firstNextRel = NeoNeoStore.unsginedInt( buffer.getInt() );
        long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
        record.setFirstNextRel( NeoNeoStore.longFromIntAndMod( firstNextRel, firstNextRelMod ) );

        long secondPrevRel = NeoNeoStore.unsginedInt( buffer.getInt() );
        long secondPrevRelMod = (typeInt & 0x380000L) << 13;
        record.setSecondPrevRel( NeoNeoStore.longFromIntAndMod( secondPrevRel, secondPrevRelMod ) );

        long secondNextRel = NeoNeoStore.unsginedInt( buffer.getInt() );
        long secondNextRelMod = (typeInt & 0x70000L) << 16;
        record.setSecondNextRel( NeoNeoStore.longFromIntAndMod( secondNextRel, secondNextRelMod ) );

        long nextProp = NeoNeoStore.unsginedInt( buffer.getInt() );
        long nextPropMod = (inUseByte & 0xF0L) << 28;

        record.setNextProp( NeoNeoStore.longFromIntAndMod( nextProp, nextPropMod ) );
        return record;
    }
}