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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyArrayStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoRelationshipStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStringStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoTokenStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

/**
 * Reader for a database in an older store format version. 
 * 
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the reader code is specific for the current upgrade and changes with each store format version.
 * 
 * {@link #LEGACY_VERSION} marks which version it's able to read.
 */
public class LegacyStore implements Closeable
{
    public static final String LEGACY_VERSION = "v0.A.0";

    private final File path;
    private final Collection<Closeable> allStoreReaders = new ArrayList<Closeable>();
    private LegacyNodeStoreReader nodeStoreReader;
    private LegacyPropertyIndexStoreReader propertyIndexReader;
    private LegacyPropertyStoreReader propertyStoreReader;

    private final FileSystemAbstraction fs;

    public LegacyStore( FileSystemAbstraction fs, File path ) throws IOException
    {
        this.fs = fs;
        this.path = path;
        assertLegacyAndCurrentVersionHaveSameLength( LEGACY_VERSION, NeoNeoStore.ALL_STORES_VERSION ); 
        initStorage();
    }
    
    /**
     * Store files that don't need migration are just copied and have their trailing versions replaced
     * by the current version. For this to work the legacy version and the current version must have the 
     * same encoded length.
     */
    static void assertLegacyAndCurrentVersionHaveSameLength( String legacyVersion, String currentVersion )
    {
        if ( UTF8.encode( legacyVersion ).length != UTF8.encode( currentVersion ).length )
            throw new IllegalStateException( "Encoded version string length must remain the same between versions" );
    }

    protected void initStorage() throws IOException
    {
        allStoreReaders.add( nodeStoreReader = new LegacyNodeStoreReader( fs, new File( path, getBaseFileName() + StoreFactory.NODE_STORE_NAME ) ) );
        allStoreReaders.add( propertyIndexReader = new LegacyPropertyIndexStoreReader( fs, new File( path, getBaseFileName() + StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ) ) );
        allStoreReaders.add( propertyStoreReader = new LegacyPropertyStoreReader( fs, new File( path, getBaseFileName() + StoreFactory.PROPERTY_STORE_NAME ) ) );
    }

    private String getBaseFileName()
    {
        return StoreFactory.NEO_STORE_NAME;
    }
    
    public static long getUnsignedInt(ByteBuffer buf)
    {
        return buf.getInt()&0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
    }

    @Override
    public void close() throws IOException
    {
        for ( Closeable storeReader : allStoreReaders )
            storeReader.close();
    }

    private void copyStore( File targetBaseStorageFileName, String storeNamePart, String versionTrailer )
            throws IOException
    {
        File targetStoreFileName = new File( targetBaseStorageFileName.getPath() + storeNamePart );
        fs.copyFile( new File( path, getBaseFileName() + storeNamePart ), targetStoreFileName );
        
        setStoreVersionTrailer( targetStoreFileName, versionTrailer );
        
        fs.copyFile(
                new File( path, getBaseFileName() + storeNamePart + ".id" ),
                new File( targetBaseStorageFileName + storeNamePart + ".id" ) );
    }

    private void setStoreVersionTrailer( File targetStoreFileName, String versionTrailer ) throws IOException
    {
        FileChannel fileChannel = fs.open( targetStoreFileName, "rw" );
        try
        {
            byte[] trailer = UTF8.encode( versionTrailer );
            fileChannel.position( fileChannel.size()-trailer.length );
            fileChannel.write( ByteBuffer.wrap( trailer ) );
        }
        finally
        {
            fileChannel.close();
        }
    }

    public void copyNeoStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.NEO_STORE_NAME, 
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoNeoStore.TYPE_DESCRIPTOR ) );
    }

    public void copyRelationshipStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.RELATIONSHIP_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoRelationshipStore.TYPE_DESCRIPTOR ) );
    }

    public void copyRelationshipTypeTokenStore(  FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_TYPE_DESCRIPTOR ) );
    }

    public void copyRelationshipTypeTokenNameStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoPropertyStringStore.TYPE_DESCRIPTOR ) );
    }

    public void copyPropertyStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.PROPERTY_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoPropertyStore.TYPE_DESCRIPTOR ) );
   }

    public void copyPropertyKeyTokenStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoTokenStore.PROPERTY_KEY_TOKEN_TYPE_DESCRIPTOR ) );
    }

    public void copyPropertyKeyTokenNameStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoPropertyStringStore.TYPE_DESCRIPTOR ) );
    }

    public void copyDynamicStringPropertyStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.PROPERTY_STRINGS_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoPropertyStringStore.TYPE_DESCRIPTOR ) );  
    }

    public void copyDynamicArrayPropertyStore( FlatNeoStores neoStores ) throws IOException
    {
        copyStore( new File( neoStores.getPath() ), StoreFactory.PROPERTY_ARRAYS_STORE_NAME,
                NeoNeoStore.buildTypeDescriptorAndVersion( NeoPropertyArrayStore.TYPE_DESCRIPTOR ) );  
    }

    public LegacyNodeStoreReader getNodeStoreReader()
    {
        return nodeStoreReader;
    }
    
    public LegacyPropertyIndexStoreReader getPropertyIndexReader()
    {
        return propertyIndexReader;
    }
    
    public LegacyPropertyStoreReader getPropertyStoreReader()
    {
        return propertyStoreReader;
    }
    
    static void readIntoBuffer( FileChannel fileChannel, ByteBuffer buffer, int nrOfBytes )
    {
        buffer.clear();
        buffer.limit( nrOfBytes );
        try
        {
            fileChannel.read( buffer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        buffer.flip();
    }
}
