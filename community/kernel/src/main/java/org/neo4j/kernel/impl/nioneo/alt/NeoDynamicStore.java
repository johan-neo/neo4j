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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.AbstractStore;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordLoad;

/**
 * An abstract representation of a dynamic store. The difference between a
 * normal {@link AbstractStore} and a <CODE>AbstractDynamicStore</CODE> is
 * that the size of a record/entry can be dynamic.
 * <p>
 * Instead of a fixed record this class uses blocks to store a record. If a
 * record size is greater than the block size the record will use one or more
 * blocks to store its data.
 * <p>
 * A dynamic store don't have a {@link IdGenerator} because the position of a
 * record can't be calculated just by knowing the id. Instead one should use a
 * {@link AbstractStore} and store the start block of the record located in the
 * dynamic store. Note: This class makes use of an id generator internally for
 * managing free and non free blocks.
 * <p>
 * Note, the first block of a dynamic store is reserved and contains information
 * about the store.
 */
public abstract class NeoDynamicStore
{
    public static final byte[] NO_DATA = new byte[0];


    /**
     * Calculate the size of a dynamic record given the size of the data block.
     *
     * @param dataSize the size of the data block in bytes.
     * @return the size of a dynamic record.
     */
    public static int getRecordSize( int dataSize )
    {
        return dataSize + BLOCK_HEADER_SIZE;
    }

    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int BLOCK_HEADER_SIZE = 1 + 3 + 4; // = 8

    public static byte[] writeToByteArray( DynamicRecord record )
    {
        byte[] data = new byte[ 4 + 4 + record.getData().length ];
        // registerIdFromUpdateRecord( blockId );
        ByteBuffer buffer = ByteBuffer.wrap( data );
        if ( record.inUse() )
        {
            long nextBlock = record.getNextBlock();
            int highByteInFirstInteger = nextBlock == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (int) ( ( nextBlock & 0xF00000000L ) >> 8 );
            highByteInFirstInteger |= ( Record.IN_USE.byteValue() << 28 );
            highByteInFirstInteger |= (record.isStartRecord() ? 0 : 1) << 31;

            /*
             * First 4b
             * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
             * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
             * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
             * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
             *
             */
            int firstInteger = record.getLength();
            assert firstInteger < ( 1 << 24 ) - 1;

            firstInteger |= highByteInFirstInteger;

            buffer.putInt( firstInteger ).putInt( (int) nextBlock );
            if ( !record.isLight() )
            {
                buffer.put( record.getData() );
            }
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
        }
        return data;
    }

    // [next][type][data]

//    static Collection<DynamicRecord> allocateRecordsFromBytes( byte src[] )
//    {
//        return allocateRecordsFromBytes( src, Collections.<DynamicRecord>emptyList().iterator(),
//                recordAllocator );
//    }

    public static Collection<DynamicRecord> allocateRecordsFromBytes(
            byte src[], Collection<DynamicRecord> recordsToUseFirst,
            DynamicRecordAllocator dynamicRecordAllocator )
    {
        assert src != null : "Null src argument";
        List<DynamicRecord> recordList = new LinkedList<>();
        DynamicRecord nextRecord = dynamicRecordAllocator.nextUsedRecordOrNew( recordsToUseFirst.iterator() );
        int srcOffset = 0;
        int dataSize = dynamicRecordAllocator.dataSize();
        do
        {
            DynamicRecord record = nextRecord;
            record.setStartRecord( srcOffset == 0 );
            if ( src.length - srcOffset > dataSize )
            {
                byte data[] = new byte[dataSize];
                System.arraycopy( src, srcOffset, data, 0, dataSize );
                record.setData( data );
                nextRecord = dynamicRecordAllocator.nextUsedRecordOrNew( recordsToUseFirst.iterator() );
                record.setNextBlock( nextRecord.getId() );
                srcOffset += dataSize;
            }
            else
            {
                byte data[] = new byte[src.length - srcOffset];
                System.arraycopy( src, srcOffset, data, 0, data.length );
                record.setData( data );
                nextRecord = null;
                record.setNextBlock( Record.NO_NEXT_BLOCK.intValue() );
            }
            recordList.add( record );
            assert !record.isLight();
            assert record.getData() != null;
        }
        while ( nextRecord != null );
        return recordList;
    }

    static boolean isRecordInUse( ByteBuffer buffer )
    {
        return ( ( buffer.get() & (byte) 0xF0 ) >> 4 ) == Record.IN_USE.byteValue();
    }

    public static void updateRecord( DynamicRecord record, byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        if ( record.inUse() )
        {
            long nextBlock = record.getNextBlock();
            int highByteInFirstInteger = nextBlock == Record.NO_NEXT_BLOCK.intValue() ? 0
                    : (int) ( ( nextBlock & 0xF00000000L ) >> 8 );
            highByteInFirstInteger |= ( Record.IN_USE.byteValue() << 28 );
            highByteInFirstInteger |= (record.isStartRecord() ? 0 : 1) << 31;

            /*
             * First 4b
             * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
             * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
             * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
             * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
             *
             */
            int firstInteger = record.getLength();
            assert firstInteger < ( 1 << 24 ) - 1;

            firstInteger |= highByteInFirstInteger;

            buffer.putInt( firstInteger ).putInt( (int) nextBlock );
            buffer.put( record.getData() );
        }
        else
        {
            buffer.put( Record.NOT_IN_USE.byteValue() );
        }
    }

    
    public static DynamicRecord getRecord( long blockId, byte[] data, RecordLoad load )
    {
        DynamicRecord record = new DynamicRecord( blockId );
        ByteBuffer buffer = ByteBuffer.wrap( data );

        /*
         * First 4b
         * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record
         *
         */
        long firstInteger = NeoNeoStore.unsginedInt( buffer.getInt() );
        boolean isStartRecord = (firstInteger & 0x80000000) == 0;
        long maskedInteger = firstInteger & ~0x80000000;
        int highNibbleInMaskedInteger = (int) ( ( maskedInteger ) >> 28 );
        boolean inUse = highNibbleInMaskedInteger == Record.IN_USE.intValue();
        if ( !inUse && load != RecordLoad.FORCE )
        {
            throw new InvalidRecordException( "DynamicRecord Not in use, blockId[" + blockId + "]" );
        }
        int dataSize = data.length - BLOCK_HEADER_SIZE;

        int nrOfBytes = (int) ( firstInteger & 0xFFFFFF );

        /*
         * Pointer to next block 4b (low bits of the pointer)
         */
        long nextBlock = NeoNeoStore.unsginedInt( buffer.getInt() );
        long nextModifier = ( firstInteger & 0xF000000L ) << 8;

        long longNextBlock = NeoNeoStore.longFromIntAndMod( nextBlock, nextModifier );
        boolean readData = load != RecordLoad.CHECK;
        if ( longNextBlock != Record.NO_NEXT_BLOCK.intValue()
            && nrOfBytes < dataSize || nrOfBytes > dataSize )
        {
            readData = false;
            if ( load != RecordLoad.FORCE )
                throw new InvalidRecordException( "Next block set[" + nextBlock
                + "] current block illegal size[" + nrOfBytes + "/" + dataSize + "]" );
        }
        record.setInUse( inUse );
        record.setStartRecord( isStartRecord );
        record.setLength( nrOfBytes );
        record.setNextBlock( longNextBlock );
        /*
         * Data 'nrOfBytes' bytes
         */
        if ( readData )
        {
            byte byteArrayElement[] = new byte[nrOfBytes];
            buffer.get( byteArrayElement );
            record.setData( byteArrayElement );
        }
        return record;
    }

 
    static public Long getNextRecordReference( DynamicRecord record )
    {
        long nextId = record.getNextBlock();
        return Record.NO_NEXT_BLOCK.is( nextId ) ? null : nextId;
    }

    /**
     * @return a {@link ByteBuffer#slice() sliced} {@link ByteBuffer} wrapping {@code target} or,
     * if necessary a new larger {@code byte[]} and containing exactly all concatenated data read from records
     */
    public static ByteBuffer concatData( Collection<DynamicRecord> records, byte[] target )
    {
        int totalLength = 0;
        for ( DynamicRecord record : records )
            totalLength += record.getLength();
        
        if ( target.length < totalLength )
            target = new byte[totalLength];
        
        ByteBuffer buffer = ByteBuffer.wrap( target );
        for ( DynamicRecord record : records )
            buffer.put( record.getData() );
        return ByteBuffer.wrap( target, 0, totalLength );
    }

    public static byte[] readFullByteArray( Iterable<DynamicRecord> records )
    {
        List<byte[]> byteList = new LinkedList<>();
        int totalSize = 0;
        for ( DynamicRecord record : records )
        {
            byteList.add( record.getData() );
            totalSize += record.getData().length;
        }
        byte[] bArray = new byte[totalSize];
        int offset = 0;
        for ( byte[] currentArray : byteList )
        {
            System.arraycopy( currentArray, 0, bArray, offset, currentArray.length );
            offset += currentArray.length;
        }
        return bArray;
    }

    public static Collection<DynamicRecord> getRecords( RecordStore store, long startBlockId, RecordLoad loadFlag )
    {
        List<DynamicRecord> recordList = new LinkedList<>();
        long blockId = startBlockId;
        while ( blockId != Record.NO_NEXT_BLOCK.intValue() )
        {
            byte[] data = store.getRecord( blockId );
            DynamicRecord record = getRecord( blockId, data, loadFlag );
            if ( ! record.inUse() )
            {
                return recordList;
            }
            recordList.add( record );
            blockId = record.getNextBlock();
        }
        return recordList;
    }
    
    public static byte[] readByteArray( RecordStore store, long blockId )
    {
        return readFullByteArray( getRecords( store, blockId, RecordLoad.NORMAL ) );
    }

    @Deprecated
    public static void ensureHeavy( DynamicRecord record, RecordStore store )
    {
        if ( !record.isLight() )
            return;
        if ( record.getLength() == 0 ) // don't go though the trouble of acquiring the window if we would read nothing
        {
            record.setData( NO_DATA );
        }

        long blockId = record.getId();
        byte[] recordData = store.getRecord( blockId );
        ByteBuffer buf = ByteBuffer.wrap( recordData );
        // NOTE: skip of header in offset
        buf.position( BLOCK_HEADER_SIZE );
        byte bytes[] = new byte[record.getLength()];
        buf.get( bytes );
        record.setData( bytes );
    }
}
