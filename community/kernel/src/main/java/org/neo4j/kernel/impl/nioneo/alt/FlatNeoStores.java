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
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public class FlatNeoStores
{
    private final String path;
    private final Config config;
    private final IdGeneratorFactory idGeneratorfactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StringLogger stringLogger;
    
    private final NeoNeoStore neoStore;
    private final NeoNodeStore nodeStore;
    private final NeoRelationshipStore relationshipStore;
    private final NeoPropertyStore propertyStore;
    private final NeoPropertyStringStore stringStore;
    private final NeoPropertyArrayStore arrayStore;
    private final NeoLabelStore labelStore;
    private final NeoSchemaStore schemaStore;
    
    private final NeoTokenStore propertyToken;
    private final NeoTokenNameStore propertyTokenName;
    private final NeoTokenStore relationshipTypeToken;
    private final NeoTokenNameStore relationshipTypeTokenName;
    private final NeoTokenStore labelToken;
    private final NeoTokenNameStore labelTokenName;
    
    
    
    public FlatNeoStores( String path, Config config, IdGeneratorFactory idGeneratorFactory, FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger )
    {
        this.path = path;

        this.config = config;
        this.idGeneratorfactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
        StoreParameter sp = new StoreParameter( path, config, idGeneratorFactory, fileSystemAbstraction, stringLogger );
        
        this.neoStore = new NeoNeoStore( sp ); 
        this.nodeStore = new NeoNodeStore( sp ); 
        this.relationshipStore = new NeoRelationshipStore( sp ); 
        this.propertyStore = new NeoPropertyStore( sp );
        this.stringStore = new NeoPropertyStringStore( sp );
        this.arrayStore = new NeoPropertyArrayStore( sp );
        this.labelStore = new NeoLabelStore( sp );
        this.schemaStore = new NeoSchemaStore( sp );
        
        this.propertyToken = new NeoTokenStore( sp, StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME, IdType.PROPERTY_KEY_TOKEN, NeoTokenStore.PROPERTY_KEY_TOKEN_TYPE_DESCRIPTOR, NeoTokenStore.PROPERTY_KEY_TOKEN_RECORD_SIZE );
        this.propertyTokenName = new NeoTokenNameStore( sp, StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME, IdType.PROPERTY_KEY_TOKEN_NAME, NeoTokenNameStore.TYPE_DESCRIPTOR  );
        this.relationshipTypeToken = new NeoTokenStore( sp, StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME, IdType.RELATIONSHIP_TYPE_TOKEN, NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_TYPE_DESCRIPTOR, NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_RECORD_SIZE );
        this.relationshipTypeTokenName = new NeoTokenNameStore( sp, StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME, IdType.RELATIONSHIP_TYPE_TOKEN_NAME, NeoTokenNameStore.TYPE_DESCRIPTOR );
        this.labelToken = new NeoTokenStore( sp, StoreFactory.LABEL_TOKEN_STORE_NAME, IdType.LABEL_TOKEN, NeoTokenStore.LABEL_TOKEN_TYPE_DESCRIPTOR, NeoTokenStore.LABEL_TOKEN_RECORD_SIZE );
        this.labelTokenName = new NeoTokenNameStore( sp, StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME, IdType.LABEL_TOKEN_NAME, NeoTokenNameStore.TYPE_DESCRIPTOR );
    }
    
    public void close()
    {
        neoStore.close();
        nodeStore.close();
        relationshipStore.close();
        propertyStore.close();
        stringStore.close();
        arrayStore.close();
        labelStore.close();
        schemaStore.close();
        
        propertyToken.close();
        propertyTokenName.close();
        relationshipTypeToken.close();
        relationshipTypeTokenName.close();
        labelToken.close();
        labelTokenName.close();
    }

    public void flushAll()
    {
        neoStore.getRecordStore().force();
        nodeStore.getRecordStore().force();
        relationshipStore.getRecordStore().force();
        propertyStore.getRecordStore().force();
        stringStore.getRecordStore().force();
        arrayStore.getRecordStore().force();
        labelStore.getRecordStore().force();
        schemaStore.getRecordStore().force();
        
        propertyToken.getRecordStore().force();
        propertyTokenName.getRecordStore().force();
        relationshipTypeToken.getRecordStore().force();
        relationshipTypeTokenName.getRecordStore().force();
        labelToken.getRecordStore().force();
        labelTokenName.getRecordStore().force();
    }
    
    public NeoNeoStore getNeoStore()
    {
        return neoStore;
    }
    
    public NeoNodeStore getNodeStore()
    {
        return nodeStore;
    }
    
    public NeoLabelStore getLabelStore()
    {
        return labelStore;
    }
    
    public NeoRelationshipStore getRelationshipStore()
    {
        return relationshipStore;
    }
    
    public NeoPropertyStore getPropertyStore()
    {
        return propertyStore;
    }
    
    public NeoPropertyStringStore getStringStore()
    {
        return stringStore;
    }
    
    public NeoPropertyArrayStore getArrayStore()
    {
        return arrayStore;
    }
    
    public NeoSchemaStore getSchemaStore()
    {
        return schemaStore;
    }
    
    public NeoTokenStore getPropertyKeyTokenStore()
    {
        return propertyToken;
    }
    
    public NeoTokenNameStore getPropertyKeyTokenNameStore()
    {
        return propertyTokenName;
    }
    
    public NeoTokenStore getRelationshipTypeTokenStore()
    {
        return relationshipTypeToken;
    }
    
    public NeoTokenNameStore getRelationshipTypeTokenNameStore()
    {
        return relationshipTypeTokenName;
    }
    
    public NeoTokenStore getLabelTokenStore()
    {
        return labelToken;
    }
    
    public NeoTokenNameStore getLabelTokenNameStore()
    {
        return labelTokenName;
    }
    
//    public String getBaseFileName()
//    {
//        return baseFileName;
//    }
    
    public String getPath()
    {
        return path;
    }
}
