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

import static java.lang.System.arraycopy;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.ShortArray;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.Bits;

/**
 * Dynamic store that stores strings.
 */
public class NeoPropertyArrayStore extends Store
{
    static final int NUMBER_HEADER_SIZE = 3;
    static final int STRING_HEADER_SIZE = 5;
    
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "ArrayPropertyStore";
    public static final String VERSION = NeoNeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );

    public static final IdType ID_TYPE = IdType.STRING_BLOCK;
    
    public NeoPropertyArrayStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.PROPERTY_ARRAYS_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, true, NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE );
    }
    
    public static Collection<DynamicRecord> allocateFromNumbers( Object array, Collection<DynamicRecord> recordsToUseFirst,
                                                                 DynamicRecordAllocator recordAllocator )
    {
        Class<?> componentType = array.getClass().getComponentType();
        boolean isPrimitiveByteArray = componentType.equals( Byte.TYPE );
        boolean isByteArray = componentType.equals( Byte.class ) || isPrimitiveByteArray;
        ShortArray type = ShortArray.typeOf( array );
        if ( type == null ) throw new IllegalArgumentException( array + " not a valid array type." );
        
        int arrayLength = Array.getLength( array );
        int requiredBits = isByteArray ? Byte.SIZE : type.calculateRequiredBitsForArray( array, arrayLength);
        int totalBits = requiredBits*arrayLength;
        int numberOfBytes = (totalBits-1)/8+1;
        int bitsUsedInLastByte = totalBits%8;
        bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
        numberOfBytes += NUMBER_HEADER_SIZE; // type + rest + requiredBits header. TODO no need to use full bytes
        byte[] bytes;
        if ( isByteArray )
        {
            bytes = new byte[NUMBER_HEADER_SIZE+ arrayLength];
            bytes[0] = (byte) type.intValue();
            bytes[1] = (byte) bitsUsedInLastByte;
            bytes[2] = (byte) requiredBits;
            if ( isPrimitiveByteArray ) arraycopy( array, 0, bytes, NUMBER_HEADER_SIZE, arrayLength );
            else
            {
                Byte[] source = (Byte[]) array;
                for ( int i = 0; i < source.length; i++ ) bytes[NUMBER_HEADER_SIZE+i] = source[i];
            }
        }
        else
        {
            Bits bits = Bits.bits( numberOfBytes );
            bits.put( (byte)type.intValue() );
            bits.put( (byte)bitsUsedInLastByte );
            bits.put( (byte)requiredBits );
            type.writeAll(array, arrayLength,requiredBits,bits);
            bytes = bits.asBytes();
        }
        return NeoDynamicStore.allocateRecordsFromBytes( bytes, recordsToUseFirst.iterator(), recordAllocator );
    }

    public static Collection<DynamicRecord> allocateFromString( String[] array, Collection<DynamicRecord> recordsToUseFirst,
                                                                 DynamicRecordAllocator recordAllocator )
    {
        List<byte[]> stringsAsBytes = new ArrayList<>();
        int totalBytesRequired = STRING_HEADER_SIZE; // 1b type + 4b array length
        for ( String string : array )
        {
            byte[] bytes = NeoPropertyStringStore.encodeString( string );
            stringsAsBytes.add( bytes );
            totalBytesRequired += 4/*byte[].length*/ + bytes.length;
        }

        ByteBuffer buf = ByteBuffer.allocate( totalBytesRequired );
        buf.put( PropertyType.STRING.byteValue() );
        buf.putInt( array.length );
        for ( byte[] stringAsBytes : stringsAsBytes )
        {
            buf.putInt( stringAsBytes.length );
            buf.put( stringAsBytes );
        }
        return NeoDynamicStore.allocateRecordsFromBytes( buf.array(), recordsToUseFirst.iterator(), recordAllocator );
    }

    public static Object getRightArray( byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        byte typeId = buffer.get();
        if ( typeId == PropertyType.STRING.intValue() )
        {
            int arrayLength = buffer.getInt();
            String[] result = new String[arrayLength];
            
            for ( int i = 0; i < arrayLength; i++ )
            {
                int byteLength = buffer.getInt();
                byte[] stringByteArray = new byte[byteLength];
                buffer.get( stringByteArray );
                result[i] = NeoPropertyStringStore.decodeString( stringByteArray );
            }
            return result;
        }
        else
        {
            ShortArray type = ShortArray.typeOf( typeId );
            int bitsUsedInLastByte = buffer.get(); 
            int requiredBits = buffer.get(); 
            if ( requiredBits == 0 )
                return type.createEmptyArray();
            if ( type == ShortArray.BYTE && requiredBits == Byte.SIZE )
            {   // Optimization for byte arrays (probably large ones)
                byte[] result = new byte[data.length - 3];
                buffer.get( result );
                return result;
            }
            else
            {   // Fallback to the generic approach, which is a slower
                int dataLength = data.length - 3;
                Bits bits = Bits.bitsFromBytes( data, 3 );
                int length = (dataLength*8-(8-bitsUsedInLastByte))/requiredBits;
                return type.createArray(length, bits, requiredBits);
            }
        }
    }

    public static Pair<byte[], byte[]> readFullByteArrayAsPair( byte[] data )
    {
        byte[] header = new byte[3];
        byte[] result = new byte[data.length - header.length];
        System.arraycopy( data, 0, header, 0, header.length );
        System.arraycopy( data, 3, result, 0, result.length );
        return Pair.of( header, data );
    }
}
