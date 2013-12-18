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

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;


/**
 * Dynamic store that stores strings.
 */
public class NeoPropertyStringStore extends Store
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "StringPropertyStore";
    public static final String VERSION = NeoNeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
    
    public static final IdType ID_TYPE = IdType.STRING_BLOCK;
    
    public NeoPropertyStringStore( StoreParameter po )
    {
        super( new File( po.path, StoreFactory.PROPERTY_STRINGS_STORE_NAME ), po.config, ID_TYPE, po.idGeneratorFactory, 
                po.fileSystemAbstraction, po.stringLogger, TYPE_DESCRIPTOR, true, NeoDynamicStore.getRecordSize( NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE ) );
    }
    
    public static byte[] encodeString( String string )
    {
        return UTF8.encode( string );
    }

    public static String decodeString( byte[] byteArray )
    {
        return UTF8.decode( byteArray );
    }
}
