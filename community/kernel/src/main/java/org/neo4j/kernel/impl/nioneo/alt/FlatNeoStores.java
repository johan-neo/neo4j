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
    private final String baseFileName;
    private final Config config;
    private final IdGeneratorFactory idGeneratorfactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StringLogger stringLogger;
    
    private final Store neoStore;
    private final Store nodeStore;
    private final Store relationshipStore;
    private final Store propertyStore;
    private final Store stringStore;
    private final Store arrayStore;
    private final Store labelStore;
    private final Store schemaStore;
    
    private final Store propertyToken;
    private final Store propertyTokenName;
    private final Store relationshipTypeToken;
    private final Store relationshipTypeTokenName;
    private final Store labelToken;
    private final Store labelTokenName;
    
    
    
    public FlatNeoStores( String fileName, Config config, IdGeneratorFactory idGeneratorFactory, FileSystemAbstraction fileSystemAbstraction,
            StringLogger stringLogger )
    {
        this.baseFileName = fileName + "neostore";
        this.config = config;
        this.idGeneratorfactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
        
        this.neoStore = createRecordStore( "", IdType.NEOSTORE_BLOCK, false, NeoNeoStore.RECORD_SIZE, NeoNeoStore.TYPE_DESCRIPTOR );
        this.nodeStore = createRecordStore( StoreFactory.NODE_STORE_NAME, IdType.NODE, false, NeoNodeStore.RECORD_SIZE, NeoNodeStore.TYPE_DESCRIPTOR );
        this.relationshipStore = createRecordStore( StoreFactory.RELATIONSHIP_STORE_NAME, IdType.RELATIONSHIP, false, NeoRelationshipStore.RECORD_SIZE, NeoRelationshipStore.TYPE_DESCRIPTOR ); 
        this.propertyStore = createRecordStore( StoreFactory.PROPERTY_STORE_NAME, IdType.PROPERTY, false, NeoPropertyStore.RECORD_SIZE, NeoPropertyStore.TYPE_DESCRIPTOR );
        this.stringStore = createRecordStore( StoreFactory.PROPERTY_STRINGS_STORE_NAME, IdType.STRING_BLOCK, true, NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE, NeoStringStore.TYPE_DESCRIPTOR );
        this.arrayStore = createRecordStore( StoreFactory.PROPERTY_ARRAYS_STORE_NAME, IdType.ARRAY_BLOCK, true, NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE, NeoArrayStore.TYPE_DESCRIPTOR );
        this.labelStore = createRecordStore( StoreFactory.NODE_LABELS_STORE_NAME, IdType.NODE_LABELS, true, NeoLabelStore.DEFAULT_DATA_BLOCK_SIZE, NeoLabelStore.TYPE_DESCRIPTOR );
        this.schemaStore = createRecordStore( StoreFactory.SCHEMA_STORE_NAME, IdType.SCHEMA, true, NeoSchemaStore.BLOCK_SIZE, NeoSchemaStore.TYPE_DESCRIPTOR );
        
        this.propertyToken = createRecordStore( StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME, IdType.PROPERTY_KEY_TOKEN, false, NeoTokenStore.PROPERTY_KEY_TOKEN_RECORD_SIZE, NeoTokenStore.PROPERTY_KEY_TOKEN_TYPE_DESCRIPTOR );
        this.propertyTokenName = createRecordStore( StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME, IdType.PROPERTY_KEY_TOKEN_NAME, true, NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE, NeoStringStore.TYPE_DESCRIPTOR );
        this.relationshipTypeToken = createRecordStore( StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME, IdType.RELATIONSHIP_TYPE_TOKEN, false, NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_RECORD_SIZE, NeoTokenStore.RELATIONSHIP_TYPE_TOKEN_TYPE_DESCRIPTOR );
        this.relationshipTypeTokenName = createRecordStore( StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME, IdType.RELATIONSHIP_TYPE_TOKEN_NAME, true, NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE, NeoStringStore.TYPE_DESCRIPTOR );
        this.labelToken = createRecordStore( StoreFactory.LABEL_TOKEN_STORE_NAME, IdType.LABEL_TOKEN, false, NeoTokenStore.LABEL_TOKEN_RECORD_SIZE, NeoTokenStore.LABEL_TOKEN_TYPE_DESCRIPTOR );
        this.labelTokenName = createRecordStore( StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME, IdType.LABEL_TOKEN_NAME, true, NeoPropertyStore.DEFAULT_DATA_BLOCK_SIZE, NeoStringStore.TYPE_DESCRIPTOR );
    }
    
    private Store createRecordStore( String fileName, IdType idType, boolean isDynamic, int recordSize, String storeTypeDescriptor )
    {
        File file = new File( baseFileName + fileName );
        return new Store( file, config, idType, idGeneratorfactory, fileSystemAbstraction, stringLogger, storeTypeDescriptor, isDynamic, recordSize );
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
    
    public Store getNeoStore()
    {
        return neoStore;
    }
    
    public Store getNodeStore()
    {
        return nodeStore;
    }
    
    public Store getLabelStore()
    {
        return labelStore;
    }
    
    public Store getRelationshipStore()
    {
        return relationshipStore;
    }
    
    public Store getPropertyStore()
    {
        return propertyStore;
    }
    
    public Store getStringStore()
    {
        return stringStore;
    }
    
    public Store getArrayStore()
    {
        return arrayStore;
    }
    
    public Store getSchemaStore()
    {
        return schemaStore;
    }
    
    public Store getPropertyKeyTokenStore()
    {
        return propertyToken;
    }
    
    public Store getPropertyKeyTokenNameStore()
    {
        return propertyTokenName;
    }
    
    public Store getRelationshipTypeTokenStore()
    {
        return relationshipTypeToken;
    }
    
    public Store getRelationshipTypeTokenNameStore()
    {
        return relationshipTypeTokenName;
    }
    
    public Store getLabelTokenStore()
    {
        return labelToken;
    }
    
    public Store getLabelTokenNameStore()
    {
        return labelTokenName;
    }
    
    public String getBaseFileName()
    {
        return baseFileName;
    }
}
