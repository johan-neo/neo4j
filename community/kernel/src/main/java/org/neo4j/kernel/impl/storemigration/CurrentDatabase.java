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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyArrayStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNodeStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoRelationshipStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStringStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoTokenStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

public class CurrentDatabase
{
    private Map<String, String> fileNamesToTypeDescriptors = new HashMap<String, String>();

    public CurrentDatabase()
    {
        fileNamesToTypeDescriptors.put( StoreFactory.NEO_STORE_NAME, NeoNeoStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.NODE_STORE_NAME, NeoNodeStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.PROPERTY_STORE_NAME, NeoPropertyStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, NeoPropertyArrayStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME, NeoTokenStore.PROPERTY_KEY_TOKEN_TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME, NeoPropertyStringStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.PROPERTY_STRINGS_STORE_NAME, NeoPropertyStringStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.RELATIONSHIP_STORE_NAME, NeoRelationshipStore.TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME, NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_TYPE_DESCRIPTOR );
        fileNamesToTypeDescriptors.put( StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME, NeoPropertyStringStore.TYPE_DESCRIPTOR );
    }

    public boolean storeFilesAtCurrentVersion( File storeDirectory )
    {
        for ( String fileName : fileNamesToTypeDescriptors.keySet() )
        {
            String expectedVersion = NeoNeoStore.buildTypeDescriptorAndVersion( fileNamesToTypeDescriptors.get( fileName ) );
            FileChannel fileChannel = null;
            byte[] expectedVersionBytes = UTF8.encode( expectedVersion );
            try
            {
                File storeFile = new File( storeDirectory, fileName );
                if ( !storeFile.exists() )
                {
                    return false;
                }
                fileChannel = new RandomAccessFile( storeFile, "r" ).getChannel();
                fileChannel.position( fileChannel.size() - expectedVersionBytes.length );
                byte[] foundVersionBytes = new byte[expectedVersionBytes.length];
                fileChannel.read( ByteBuffer.wrap( foundVersionBytes ) );
                if ( !expectedVersion.equals( UTF8.decode( foundVersionBytes ) ) )
                {
                    return false;
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                if ( fileChannel != null )
                {
                    try
                    {
                        fileChannel.close();
                    }
                    catch ( IOException e )
                    {
                        return true;
                    }
                }
            }
        }
        return true;
    }
}
