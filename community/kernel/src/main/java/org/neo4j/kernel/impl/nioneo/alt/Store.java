package org.neo4j.kernel.impl.nioneo.alt;

import java.io.File;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.util.StringLogger;

public class Store
{
    private final StoreLoader storeLoader;
    private final IdGenerator idGenerator;
    private final RecordStore recordStore;

    public Store( File fileName, Config config, IdType idType, IdGeneratorFactory idGeneratorFactory,
            FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, String storeTypeDescriptor,
            boolean isDynamic, int recordSize)
    {
        storeLoader = new StoreLoader( fileName, config, idType, idGeneratorFactory, fileSystemAbstraction,
                stringLogger, storeTypeDescriptor, isDynamic, recordSize );
        storeLoader.load();
        if ( !storeLoader.isStoreOk() )
        {
            stringLogger.logMessage( "Store not ok " + fileName, storeLoader.getCauseStoreNotOk() );
            storeLoader.makeStoreOk();
        }
        this.idGenerator = storeLoader.getIdGenerator();
        FileWithRecords fwr = new FileWithRecords( fileName.getName(), storeLoader.getFileChannel(),
                storeLoader.getRecordSize() );
        // TODO: add to Configuration
        int targetPageSize = 4 * 1024 * 1024;
        PageType type = PageType.MEMORY_MAPPED;
        PageSynchronization refSync = PageSynchronization.ATOMIC;
        long initialMemory = 1 * 1024 * 1024 * 1024;
        //////////////////////
        this.recordStore = new PagedFileWithRecords( fwr, targetPageSize, type, refSync, initialMemory );
    }

    public void close()
    {
        recordStore.close();
        idGenerator.close();
        storeLoader.releaseFileLockAndCloseFileChannel();
    }

    public RecordStore getRecordStore()
    {
        return recordStore;
    }
    
    public IdGenerator getIdGenerator()
    {
        return idGenerator;
    }
}
