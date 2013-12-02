package org.neo4j.kernel.impl.nioneo.alt;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.StringLogger;

public class StoreParameter
{
    public String path;
    public Config config;
    public IdGeneratorFactory idGeneratorFactory;
    public FileSystemAbstraction fileSystemAbstraction;
    public StringLogger stringLogger;

    public StoreParameter( String path, Config config, IdGeneratorFactory idGeneratorFactory,
            FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger )
    {
        this.path = path;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
    }
}