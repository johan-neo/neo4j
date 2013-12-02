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

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.Bits;

public class NeoNeoStore extends Store
{
    public static abstract class Configuration
    {
        public static final Setting<Integer> relationship_grab_size = GraphDatabaseSettings.relationship_grab_size;
    }

    public static final String TYPE_DESCRIPTOR = "NeoStore";

    public static final String ALL_STORES_VERSION = "v0.A.1";

    /*
     *  7 longs in header (long + in use), time | random | version | txid | store version | graph next prop | latest constraint tx
     */
    public static final int RECORD_SIZE = 9;

    // public static final String DEFAULT_NAME = "neostore";

    public static final IdType ID_TYPE = IdType.NEOSTORE_BLOCK;
    
    // Positions of meta-data records

    public static final int TIME_POSITION = 0;
    public static final int RANDOM_POSITION = 1;
    public static final int LOG_VERSION_POSITION = 2;
    public static final int LATEST_COMMITTED_TX_POSITION = 3;
    public static final int STORE_VERSION_POSITION = 4;
    public static final int NEXT_GRAPH_PROP_POSITION = 5;
    public static final int LATEST_CONSTRAINT_TX_POSITION = 6;
    
    public NeoNeoStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.NEO_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, false, RECORD_SIZE );
    }
    
    static long unsginedInt( int value )
    {
        return value & 0xFFFFFFFFL;
    }

    static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }


    public static String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return typeDescriptor + " " + ALL_STORES_VERSION;
    }

    public static long getLong( byte[] data )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        buffer.get(); // skip the in-use byte
        return buffer.getLong();
    }
    
    public static byte[] updateLong( byte[] data, long newLong )
    {
        ByteBuffer buffer = ByteBuffer.wrap( data );
        buffer.get(); // skip the in use
        buffer.putLong( newLong );
        return data;
    }

    public static long getLong( RecordStore recordStore, long record )
    {
        byte[] data = recordStore.getRecord( record );
        return getLong( data );
    }
    
    public static void writeLong( RecordStore recordStore, long record, long value )
    {
        byte[] data = new byte[RECORD_SIZE];
        updateLong( data, value );
        recordStore.writeRecord( record, data );
    }
    
    public static final String UNKNOWN_VERSION = "Uknown";

    public static long versionStringToLong( String storeVersion )
    {
        if ( UNKNOWN_VERSION.equals( storeVersion ) )
        {
            return -1;
        }
        Bits bits = Bits.bits(8);
        int length = storeVersion.length();
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException(
                    String.format(
                            "The given string %s is not of proper size for a store version string",
                            storeVersion ) );
        }
        bits.put( length, 8 );
        for ( int i = 0; i < length; i++ )
        {
            char c = storeVersion.charAt( i );
            if ( c < 0 || c >= 256 )
                throw new IllegalArgumentException(
                        String.format(
                                "Store version strings should be encode-able as Latin1 - %s is not",
                                storeVersion ) );
            bits.put( c, 8 ); // Just the lower byte
        }
        return bits.getLong();
    }
   
    public static String versionLongToString( long storeVersion )
    {
        if ( storeVersion == -1 )
        {
            return UNKNOWN_VERSION;
        }
        Bits bits = Bits.bitsFromLongs(new long[]{storeVersion});
        int length = bits.getShort( 8 );
        if ( length == 0 || length > 7 )
        {
            throw new IllegalArgumentException( String.format(
                    "The read in version string length %d is not proper.",
                    length ) );
        }
        char[] result = new char[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (char) bits.getShort( 8 );
        }
        return new String( result );
    }
}
