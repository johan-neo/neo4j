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
        //////////////////////
        
        this.recordStore = new PagedFileWithRecords( fwr, targetPageSize, type, refSync );
    }

    public void close()
    {
        recordStore.close();
        storeLoader.writeTypeAndVersion();
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
    
    public static class NeoStore extends Store
    {
        public NeoStore( File fileName, Config config, IdType idType, IdGeneratorFactory idGeneratorFactory,
                FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, String storeTypeDescriptor,
                boolean isDynamic, int recordSize)
        {
            super( fileName, config, idType, idGeneratorFactory, fileSystemAbstraction, stringLogger, storeTypeDescriptor, isDynamic, recordSize );
        }
    }

    public static class NodeStore extends Store
    {
        public NodeStore( File fileName, Config config, IdType idType, IdGeneratorFactory idGeneratorFactory,
                FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, String storeTypeDescriptor,
                boolean isDynamic, int recordSize)
        {
            super( fileName, config, idType, idGeneratorFactory, fileSystemAbstraction, stringLogger, storeTypeDescriptor, isDynamic, recordSize );
        }
    }

    public static class RelationshipStore extends Store
    {
        public RelationshipStore( File fileName, Config config, IdType idType, IdGeneratorFactory idGeneratorFactory,
                FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, String storeTypeDescriptor,
                boolean isDynamic, int recordSize)
        {
            super( fileName, config, idType, idGeneratorFactory, fileSystemAbstraction, stringLogger, storeTypeDescriptor, isDynamic, recordSize );
        }
    }
}
