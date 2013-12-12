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

import static org.neo4j.helpers.Exceptions.launderedException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.LinkedList;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.InvalidIdGeneratorException;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.nioneo.store.ReadOnlyIdGenerator;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StoreNotFoundException;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.StringLogger;

public class StoreLoader
{
    public static abstract class Configuration
    {
        public static final Setting<File> store_dir = InternalAbstractGraphDatabase.Configuration.store_dir;

        public static final Setting<Boolean> read_only = GraphDatabaseSettings.read_only;
        public static final Setting<Boolean> backup_slave = GraphDatabaseSettings.backup_slave;
        public static final Setting<Boolean> use_memory_mapped_buffers = GraphDatabaseSettings.use_memory_mapped_buffers;
        public static final Setting<Boolean> rebuild_idgenerators_fast = GraphDatabaseSettings.rebuild_idgenerators_fast;
    }
    
    public final String storeTypeDescriptor;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final Config configuration;
    private final File storageFileName;
    private final boolean isDynamic;
    private final StringLogger stringLogger;
    private final IdType idType;
    private final IdGeneratorFactory idGeneratorFactory;
    
    private FileChannel fileChannel;
    private FileLock fileLock;
    private IdGenerator idGenerator;
    private int recordSize;
    
    private boolean storeOk = true;
    private Throwable causeOfStoreNotOk = null;

    public StoreLoader( File fileName, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory,
            FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, String storeTypeDescriptor,
            boolean isDynamic, int recordSize )
    {
        this.configuration = configuration;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.storageFileName = fileName;
        this.storeTypeDescriptor = storeTypeDescriptor;
        this.isDynamic = isDynamic;
        this.stringLogger = stringLogger;
        this.idType = idType;
        this.idGeneratorFactory = idGeneratorFactory;
        this.recordSize = recordSize;
    }

    public void load()
    {
        try
        {
            checkStorage();
            checkVersion();
            loadStorage();
        }
        catch ( Exception e )
        {
            releaseFileLockAndCloseFileChannel();
            throw launderedException( e );
        }
    }

    public void releaseFileLockAndCloseFileChannel()
    {
        try
        {
            if ( fileLock != null )
            {
                fileLock.release();
            }
            if ( fileChannel != null )
            {
                fileChannel.close();
            }
        }
        catch ( IOException e )
        {
            stringLogger.warn( "Could not close [" + storageFileName + "]", e );
        }
        fileChannel = null;
    }

    public FileChannel getFileChannel()
    {
        return fileChannel;
    }

    public IdGenerator getIdGenerator()
    {
        return idGenerator;
    }

    public int getRecordSize()
    {
        return recordSize;
    }
    private void checkStorage()
    {
        if ( !fileSystemAbstraction.fileExists( storageFileName ) )
        {
            throw new StoreNotFoundException( "No such store[" + storageFileName + "] in " + fileSystemAbstraction );
        }
        try
        {
            this.fileChannel = fileSystemAbstraction.open( storageFileName, isReadOnly() ? "r" : "rw" );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open file " + storageFileName, e );
        }
        try
        {
            if ( !isReadOnly() || isBackupSlave() )
            {
                this.fileLock = fileSystemAbstraction.tryLock( storageFileName, fileChannel );
                if ( fileLock == null )
                {
                    throw new IllegalStateException( "Unable to lock store [" + storageFileName
                            + "], this is usually a result of some "
                            + "other Neo4j kernel running using the same store." );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to lock store[" + storageFileName + "]", e );
        }
        catch ( OverlappingFileLockException e )
        {
            throw new IllegalStateException( "Unable to lock store [" + storageFileName
                    + "], this is usually caused by another Neo4j kernel already running in "
                    + "this JVM for this particular store" );
        }
    }

    private Boolean isBackupSlave()
    {
        return configuration.get( Configuration.backup_slave );
    }

    private Boolean isReadOnly()
    {
        return configuration.get( Configuration.read_only );
    }

    private void setStoreNotOk( Throwable cause )
    {
        if ( isReadOnly() && !isBackupSlave() )
        {
            throw new UnderlyingStorageException( "Cannot start up on non clean store as read only" );
        }
        storeOk = false;
        causeOfStoreNotOk = cause;
    }

    private void checkVersion()
    {
        try
        {
            verifyCorrectTypeDescriptorAndVersion();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to check version " + storageFileName, e );
        }
    }

    private void loadStorage()
    {
        try
        {
            if ( isDynamic )
            {
                readAndVerifyBlockSize();
            }
            verifyFileSizeAndTruncate();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to load storage " + storageFileName, e );
        }
        loadIdGenerator();
    }

    private void loadIdGenerator()
    {
        try
        {
            if ( !isReadOnly() || isBackupSlave() )
            {
                File fileName = new File( storageFileName.getPath() + ".id" );
                idGenerator = idGeneratorFactory
                        .open( fileSystemAbstraction, fileName, idType.getGrabSize(), idType, figureOutHighestIdInUse() );
            }
            else
            {
                idGenerator = new ReadOnlyIdGenerator( storageFileName + ".id", fileChannel.size() / recordSize );
            }
        }
        catch ( InvalidIdGeneratorException | IOException e )
        {
            setStoreNotOk( e );
        }
        finally
        {
            if ( !getStoreOk() )
            {
                if ( stringLogger != null )
                {
                    stringLogger.logMessage( storageFileName + " non clean shutdown detected", true );
                }
            }
        }
    }

    private long figureOutHighestIdInUse()
    {
        try
        {
            return getFileChannel().size() / getRecordSize();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private boolean getStoreOk()
    {
        return storeOk;
    }

    private void readAndVerifyBlockSize() throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 4 );
        getFileChannel().position( 0 );
        getFileChannel().read( buffer );
        buffer.flip();
        int blockSize = buffer.getInt();
        if ( blockSize <= 0 )
        {
            throw new InvalidRecordException( "Illegal block size: " + blockSize + " in " + storageFileName );
        }
        this.recordSize = blockSize;
    }

    private void verifyFileSizeAndTruncate() throws IOException
    {
        int expectedVersionLength = UTF8.encode( buildTypeDescriptorAndVersion( storeTypeDescriptor ) ).length;
        long fileSize = getFileChannel().size();
        if ( getRecordSize() != 0 && (fileSize - expectedVersionLength) % getRecordSize() != 0 && !isReadOnly() )
        {
            setStoreNotOk( new IllegalStateException( "Misaligned file size " + fileSize + " for " + this
                    + ", expected version length:" + expectedVersionLength ) );
        }
        if ( getStoreOk() && !isReadOnly() )
        {
            getFileChannel().truncate( fileSize - expectedVersionLength );
        }
    }

    private void verifyCorrectTypeDescriptorAndVersion() throws IOException
    {
        String expectedTypeDescriptorAndVersion = getTypeAndVersionDescriptor();
        int length = UTF8.encode( expectedTypeDescriptorAndVersion ).length;
        byte bytes[] = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap( bytes );
        long fileSize = getFileChannel().size();
        if ( fileSize >= length )
        {
            getFileChannel().position( fileSize - length );
        }
        else if ( isReadOnly() )
        {
            setStoreNotOk( new IllegalStateException( "Invalid file size " + fileSize + " for " + this + ". Expected "
                    + length + " or bigger" ) );
            return;
        }
        getFileChannel().read( buffer );
        String foundTypeDescriptorAndVersion = UTF8.decode( bytes );
        if ( !expectedTypeDescriptorAndVersion.equals( foundTypeDescriptorAndVersion ) && !isReadOnly() )
        {
            if ( foundTypeDescriptorAndVersion.startsWith( storeTypeDescriptor ) )
            {
                throw new NotCurrentStoreVersionException( NeoNeoStore.ALL_STORES_VERSION, foundTypeDescriptorAndVersion, "", false );
            }
            else
            {
                setStoreNotOk( new IllegalStateException( "Unexpected version " + foundTypeDescriptorAndVersion
                        + ", expected " + expectedTypeDescriptorAndVersion ) );
            }
        }
    }

    private String getTypeAndVersionDescriptor()
    {
        return buildTypeDescriptorAndVersion( storeTypeDescriptor );
    }

    private String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return NeoNeoStore.buildTypeDescriptorAndVersion( typeDescriptor );
    }

    public boolean isStoreOk()
    {
        return storeOk;
    }

    public Throwable getCauseStoreNotOk()
    {
        return causeOfStoreNotOk;
    }

    public void makeStoreOk()
    {
        if ( !storeOk )
        {
            if ( isReadOnly() && !isBackupSlave() )
            {
                throw new ReadOnlyDbException();
            }
            rebuildIdGenerator();
            storeOk = true;
            causeOfStoreNotOk = null;
        }
    }

    private void rebuildIdGenerator()
    {
        if ( isReadOnly() && !isBackupSlave() )
        {
            throw new ReadOnlyDbException();
        }
        if ( isDynamic && getRecordSize() <= 0 )
        {
            throw new InvalidRecordException( "Illegal blockSize: " +
                getRecordSize() );
        }
        stringLogger.debug( "Rebuilding id generator for[" + storageFileName + "] ..." );
        File idGeneratorFileName = new File( storageFileName.getPath() + ".id" );
        if ( fileSystemAbstraction.fileExists( new File( storageFileName.getPath() + ".id" ) ) )
        {
            boolean success = fileSystemAbstraction.deleteFile( new File( storageFileName.getPath() + ".id" ) );
            assert success;
        }
        idGeneratorFactory.create( fileSystemAbstraction, idGeneratorFileName, 0 );
        idGenerator = idGeneratorFactory
                .open( fileSystemAbstraction, idGeneratorFileName, idType.getGrabSize(), idType, figureOutHighestIdInUse() );
        if ( isDynamic )
        {
            idGenerator.setHighId( 1 ); // reserved first block containing blockSize
        }
        FileChannel fileChannel = getFileChannel();
        long highId = 1;
        long defraggedCount = 0;
        try
        {
            long fileSize = fileChannel.size();
            int recordSize = getRecordSize();
            boolean fullRebuild = true;
            if ( configuration.get( Configuration.rebuild_idgenerators_fast ) )
            {
                fullRebuild = false;
                highId = findHighIdBackwards();
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate( recordSize );
            // Duplicated code block
            LinkedList<Long> freeIdList = new LinkedList<Long>();
            if ( fullRebuild )
            {
                long start = 0;
                if ( isDynamic )
                {
                    start = 1;
                }
                for ( long i = start; i * recordSize < fileSize && recordSize > 0; i++ )
                {
                    fileChannel.position( i * recordSize );
                    byteBuffer.clear();
                    fileChannel.read( byteBuffer );
                    byteBuffer.flip();
                    if ( !isRecordInUse( byteBuffer ) )
                    {
                        freeIdList.add( i );
                    }
                    else
                    {
                        highId = i;
                        idGenerator.setHighId( highId + 1 );
                        while ( !freeIdList.isEmpty() )
                        {
                            idGenerator.freeId( freeIdList.removeFirst() );
                            defraggedCount++;
                        }
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to rebuild id generator " + storageFileName, e );
        }
        idGenerator.setHighId( highId + 1 );
        stringLogger.logMessage( storageFileName + " rebuild id generator, highId=" + idGenerator.getHighId() +
                                 " defragged count=" + defraggedCount, true );
        stringLogger.debug( "[" + storageFileName + "] high id=" + idGenerator.getHighId()
                            + " (defragged=" + defraggedCount + ")" );
        idGenerator.close();
        idGenerator = idGeneratorFactory
                .open( fileSystemAbstraction, idGeneratorFileName, idType.getGrabSize(), idType, figureOutHighestIdInUse() );
    }
    
    
    private long findHighIdBackwards() throws IOException
    {
        // Duplicated method
        FileChannel fileChannel = getFileChannel();
        int recordSize = getRecordSize();
        long fileSize = fileChannel.size();
        long highId = fileSize / recordSize;
        long end = 0;
        if ( isDynamic )
        {
            end = 1;
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate( getRecordSize() );
        for ( long i = highId; i > end; i-- )
        {
            fileChannel.position( i * recordSize );
            if ( fileChannel.read( byteBuffer ) > 0 )
            {
                byteBuffer.flip();
                boolean isInUse = isRecordInUse( byteBuffer );
                byteBuffer.clear();
                if ( isInUse )
                {
                    return i;
                }
            }
        }
        return 0;
    }
    
    private boolean isRecordInUse( ByteBuffer buffer )
    {
        if ( isDynamic )
        {
            return ( ( buffer.get() & (byte) 0xF0 ) >> 4 ) == Record.IN_USE.byteValue();
        }
        byte inUse = buffer.get();
        return (inUse & 0x1) == Record.IN_USE.byteValue();
    }
    
    public void writeTypeAndVersion()
    {
        boolean success = isReadOnly();
        IOException storedIoe = null;
        // hack for WINBLOWS
        if ( !isReadOnly() || isBackupSlave() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                try
                {
                    fileChannel.position( idGenerator.getHighId() * recordSize );
                    ByteBuffer buffer = ByteBuffer.wrap(
                            UTF8.encode( getTypeAndVersionDescriptor() ) );
                    fileChannel.write( buffer );
                    stringLogger.debug( "Closing " + storageFileName + ", truncating at " + fileChannel.position() +
                                        " vs file size " + fileChannel.size() );
                    fileChannel.truncate( fileChannel.position() );
                    fileChannel.force( false );
                    success = true;
                    break;
                }
                catch ( IOException e )
                {
                    storedIoe = e;
                    System.gc();
                }
            }
        }
        if ( !success )
        {
            throw new UnderlyingStorageException( "Unable to close store "
                                                  + storageFileName, storedIoe );
        }

    }
}
