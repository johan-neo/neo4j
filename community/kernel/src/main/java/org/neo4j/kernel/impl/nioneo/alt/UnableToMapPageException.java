package org.neo4j.kernel.impl.nioneo.alt;

public class UnableToMapPageException extends Exception
{
    public UnableToMapPageException( String msg )
    {
        super( msg );
    }

    public UnableToMapPageException( Throwable cause )
    {
        super( cause );
    }

    public UnableToMapPageException( String msg, Throwable cause )
    {
        super( msg, cause );
    }
}
