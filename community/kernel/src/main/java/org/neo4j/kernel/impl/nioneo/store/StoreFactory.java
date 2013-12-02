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
package org.neo4j.kernel.impl.nioneo.store;

import static org.neo4j.helpers.Settings.setting;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.alt.FlatNeoStores;
import org.neo4j.kernel.impl.nioneo.alt.NeoDynamicStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNeoStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoNodeStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyArrayStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoPropertyStringStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoRelationshipStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoSchemaStore;
import org.neo4j.kernel.impl.nioneo.alt.NeoTokenStore;
import org.neo4j.kernel.impl.nioneo.alt.RecordStore;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.DatabaseFiles;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.util.StringLogger;

/**
* Factory for Store implementations. Can also be used to create empty stores.
*/
public class StoreFactory
{
    public static abstract class Configuration
    {
        public static final Setting<Integer> string_block_size = GraphDatabaseSettings.string_block_size;
        public static final Setting<Integer> array_block_size = GraphDatabaseSettings.array_block_size;
        public static final Setting<Integer> label_block_size = GraphDatabaseSettings.label_block_size;
    }

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StringLogger stringLogger;
    private final RemoteTxHook txHook;

    public static final String NEO_STORE_NAME =                             "neostore";
    public static final String NODE_STORE_NAME =                            "neostore.nodestore.db";
    public static final String NODE_LABELS_STORE_NAME =                     "neostore.nodestore.db.labels";
    public static final String PROPERTY_STORE_NAME =                        "neostore.propertystore.db";
    public static final String PROPERTY_KEY_TOKEN_STORE_NAME =              "neostore.propertystore.db.index";
    public static final String PROPERTY_KEY_TOKEN_NAMES_STORE_NAME =        "neostore.propertystore.db.index.names";
    public static final String PROPERTY_STRINGS_STORE_NAME =                "neostore.propertystore.db.strings";
    public static final String PROPERTY_ARRAYS_STORE_NAME =                 "neostore.propertystore.db.arrays";
    public static final String RELATIONSHIP_STORE_NAME =                    "neostore.relationshipstore.db";
    public static final String RELATIONSHIP_TYPE_TOKEN_STORE_NAME =         "neostore.relationshiptypestore.db";
    public static final String RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME =   "neostore.relationshiptypestore.db.names";
    public static final String LABEL_TOKEN_STORE_NAME =                     "neostore.labeltokenstore.db";
    public static final String LABEL_TOKEN_NAMES_STORE_NAME =               "neostore.labeltokenstore.db.names"; 
    public static final String SCHEMA_STORE_NAME =                          "neostore.schemastore.db";

    public StoreFactory( Config config, IdGeneratorFactory idGeneratorFactory,
                         FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, RemoteTxHook txHook  )
    {
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
        this.txHook = txHook;
    }

    public boolean ensureStoreExists() throws IOException
    {
        boolean readOnly = config.get( GraphDatabaseSettings.read_only );

        File path = config.get( GraphDatabaseSettings.store_dir );
        boolean created = false;
        if ( !readOnly && !fileSystemAbstraction.fileExists( new File( path, NEO_STORE_NAME ) ) )
        {
            stringLogger.info( "Creating new db @ " + path );
            fileSystemAbstraction.mkdirs( path );
            createNeoStore( path.getPath() ).close();
            created = true;
        }
        return created;
    }

    public FlatNeoStores openNeoStore( String path )
    {
        try
        {
            return attemptNewFlatNeoStores( path ); 
        }
        catch ( NotCurrentStoreVersionException e )
        {
            tryToUpgradeStores( path );
            return attemptNewFlatNeoStores( path );
        }
        catch ( StoreNotFoundException e )
        {
            createNeoStore( path ).close();
            return attemptNewFlatNeoStores( path );
        }
    }
    
    FlatNeoStores attemptNewFlatNeoStores( String path )
    {
        return new FlatNeoStores( path, config, idGeneratorFactory, fileSystemAbstraction, stringLogger ); 
    }
    
    private void tryToUpgradeStores( String path )
    {
        new StoreUpgrader(config, new ConfigMapUpgradeConfiguration(config),
                new UpgradableDatabase( fileSystemAbstraction ),
                new StoreMigrator( new VisibleMigrationProgressMonitor( stringLogger, System.out ) ),
                new DatabaseFiles( fileSystemAbstraction ),
                    idGeneratorFactory, fileSystemAbstraction ).attemptUpgrade( path );
    }

    public FlatNeoStores createNeoStore( String path )
    {
        return createNeoStore( path, new StoreId() );
    } 

    public FlatNeoStores createNeoStore( String path, StoreId storeId )
    {
        File parent = new File( path ); 
        if ( parent != null && !fileSystemAbstraction.fileExists( parent ) )
        {
            try
            {
                fileSystemAbstraction.mkdirs( parent );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Unable to create directory " + parent, e );
            }
        }
        
        createEmptyStore( path, NEO_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoNeoStore.TYPE_DESCRIPTOR ) );
        createNodeStore( path );
        createRelationshipStore( path );
        createPropertyStore( path );
        createRelationshipTypeStore( path );
        createLabelTokenStore( path );
        createSchemaStore( path );

        FlatNeoStores neoStores = new FlatNeoStores( path, config, idGeneratorFactory, fileSystemAbstraction, stringLogger );
        RecordStore neoStore = neoStores.getNeoStore().getRecordStore();
     
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.TIME_POSITION, storeId.getCreationTime() );
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.RANDOM_POSITION, storeId.getRandomId() );
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.LOG_VERSION_POSITION, 0 );
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.LATEST_COMMITTED_TX_POSITION, 1 );
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.STORE_VERSION_POSITION, storeId.getStoreVersion() );
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.NEXT_GRAPH_PROP_POSITION, -1 );
        NeoNeoStore.writeLong( neoStore, NeoNeoStore.LATEST_CONSTRAINT_TX_POSITION, 0 );
        neoStores.getNeoStore().getIdGenerator().setHighId( 7 /*LATEST_CONSTRAINT_TX_POSITION + 1 */ );
        
        return neoStores;
    }

    /**
     * Creates a new node store contained in <CODE>fileName</CODE> If filename
     * is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new node store
     */
    public void createNodeStore( String path )
    {
        createNodeLabelsStore( path );
        createEmptyStore( path, NODE_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoNodeStore.TYPE_DESCRIPTOR ) );
    }

    private void createNodeLabelsStore( String path )
    {
        int labelStoreBlockSize = config.get( Configuration.label_block_size );
        createEmptyDynamicStore( path, NODE_LABELS_STORE_NAME, labelStoreBlockSize,
                NeoPropertyArrayStore.VERSION, IdType.NODE_LABELS );
    }

    /**
     * Creates a new relationship store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an <CODE>IOException</CODE>
     * is thrown.
     *
     * @param fileName
     *            File name of the new relationship store
     */
    private void createRelationshipStore( String path )
    {
        createEmptyStore( path, RELATIONSHIP_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoRelationshipStore.TYPE_DESCRIPTOR ) );
    }

    /**
     * Creates a new property store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new property store
     */
    public void createPropertyStore( String path )
    {
        createEmptyStore( path, PROPERTY_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoPropertyStore.TYPE_DESCRIPTOR ));
        int stringStoreBlockSize = config.get( Configuration.string_block_size );
        int arrayStoreBlockSize = config.get( Configuration.array_block_size );

        createDynamicStringStore( path, stringStoreBlockSize, IdType.STRING_BLOCK);
        createPropertyKeyTokenStore( path );
        createDynamicArrayStore( path, arrayStoreBlockSize );
    }

    /**
     * Creates a new relationship type store contained in <CODE>fileName</CODE>
     * If filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new relationship type store
     */
    private void createRelationshipTypeStore( String path )
    {
        createEmptyStore( path, RELATIONSHIP_TYPE_TOKEN_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_TYPE_DESCRIPTOR ));
        createTokenNameStore( path, RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME, NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_RECORD_SIZE, IdType.RELATIONSHIP_TYPE_TOKEN_NAME );
    }

    private void createLabelTokenStore( String path )
    {
        createEmptyStore( path, LABEL_TOKEN_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoTokenStore.LABEL_TOKEN_TYPE_DESCRIPTOR ));
        createTokenNameStore( path, LABEL_TOKEN_NAMES_STORE_NAME, NeoTokenStore.LABEL_TOKEN_RECORD_SIZE, IdType.LABEL_TOKEN_NAME );
    }

    private void createPropertyKeyTokenStore( String path )
    {
        createEmptyStore( path, PROPERTY_KEY_TOKEN_STORE_NAME, NeoNeoStore.buildTypeDescriptorAndVersion( NeoTokenStore.PROPERTY_KEY_TOKEN_TYPE_DESCRIPTOR ) );
        createTokenNameStore( path, PROPERTY_KEY_TOKEN_NAMES_STORE_NAME, NeoTokenStore.PROPERTY_KEY_TOKEN_RECORD_SIZE, IdType.PROPERTY_KEY_TOKEN_NAME );
    }
    
    private void createTokenNameStore( String path, String name, int blockSize, IdType idType )
    {
        createEmptyDynamicStore(path, name, blockSize, NeoPropertyStringStore.VERSION, idType);
    }
    
    private void createDynamicStringStore( String path, int blockSize, IdType idType )
    {
        createEmptyDynamicStore(path, PROPERTY_STRINGS_STORE_NAME, blockSize, NeoPropertyStringStore.VERSION, idType);
    }

    public void createDynamicArrayStore( String path, int blockSize)
    {
        createEmptyDynamicStore( path, PROPERTY_ARRAYS_STORE_NAME, blockSize, NeoPropertyArrayStore.VERSION, IdType.ARRAY_BLOCK);
    }

    public void createSchemaStore( String path )
    {
        createEmptyDynamicStore( path, SCHEMA_STORE_NAME, NeoSchemaStore.BLOCK_SIZE, NeoSchemaStore.VERSION, IdType.SCHEMA );
    }

    /**
     * Creates a new empty store. A factory method returning an implementation
     * should make use of this method to initialize an empty store. Block size
     * must be greater than zero. Not that the first block will be marked as
     * reserved (contains info about the block size). There will be an overhead
     * for each block of <CODE>AbstractDynamicStore.BLOCK_HEADER_SIZE</CODE>
     * bytes.
     * <p>
     * This method will create a empty store with descriptor returned by the
     * {@link CommonAbstractStore#getTypeDescriptor()}. The internal id generator used by
     * this store will also be created.
     *
     * @param fileName
     *            The file name of the store that will be created
     * @param  baseBlockSize
     *            The number of bytes for each block
     * @param typeAndVersionDescriptor
     *            The type and version descriptor that identifies this store
     */
    public void createEmptyDynamicStore( String path, String name, int baseBlockSize,
                                            String typeAndVersionDescriptor, IdType idType)
    {
        File fileName = new File( path, name );
        int blockSize = baseBlockSize;
        if ( fileSystemAbstraction.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                    + "], file already exists" );
        }
        if ( blockSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal block size["
                    + blockSize + "]" );
        }
        if ( blockSize > 0xFFFF )
        {
            throw new IllegalArgumentException( "Illegal block size[" + blockSize + "], limit is 65535" );
        }
        blockSize += NeoDynamicStore.BLOCK_HEADER_SIZE;

        // write the header
        try
        {
            FileChannel channel = fileSystemAbstraction.create(fileName);
            int endHeaderSize = blockSize
                    + UTF8.encode( typeAndVersionDescriptor ).length;
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            buffer.putInt( blockSize );
            buffer.position( endHeaderSize - typeAndVersionDescriptor.length() );
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store "
                    + fileName, e );
        }
        idGeneratorFactory.create( fileSystemAbstraction, new File( fileName.getPath() + ".id"), 0 );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open(fileSystemAbstraction, new File( fileName.getPath() + ".id"),
                idType.getGrabSize(), idType, 0 );
        idGenerator.nextId(); // reserve first for blockSize
        idGenerator.close();
    }


    public void createEmptyStore( String path, String name, String typeAndVersionDescriptor)
    {
        File fileName = new File( path, name );
        // sanity checks
        if ( fileSystemAbstraction.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                    + "], file already exists" );
        }

        // write the header
        try
        {
            FileChannel channel = fileSystemAbstraction.create(fileName);
            int endHeaderSize = UTF8.encode(typeAndVersionDescriptor).length;
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store "
                    + fileName, e );
        }
        idGeneratorFactory.create( fileSystemAbstraction, new File( fileName.getPath() + ".id"), 0 );
    }

    public static Setting<Long> memoryMappingSetting( String fileName )
    {
        return setting( fileName + ".mapped_memory", Settings.BYTES, Settings.NO_DEFAULT );
    }
}
