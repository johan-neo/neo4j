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
import java.util.LinkedList;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;

/**
 * Implementation of the property store.
 */
public class NeoTokenNameStore extends Store
{
    public static final int NAME_STORE_BLOCK_SIZE = 30;
    public static final String TYPE_DESCRIPTOR = "StringPropertyStore";
    
    public NeoTokenNameStore( StoreParameter po, String fileName, IdType idType, String typeDescriptor )
    {
        super( new File( po.path, fileName ), po.config, idType, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, typeDescriptor, true, NAME_STORE_BLOCK_SIZE );
    }
}