package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;

public class PhysicalLogPiggybackWriterThread extends Thread
{
    private volatile boolean run = true;
    private final PhysicalTransactionAppender appender;

    public PhysicalLogPiggybackWriterThread( PhysicalTransactionAppender appender )
    {
        super( "PhysicalLogPiggybackWriterThread" );
        this.appender = appender;
    }

    public void stopRunningBatchThread()
    {
        run = false;
    }
    
    @Override
    public void run()
    {
        while ( run )
        {
            try
            {
                appender.flushAndRelease();
            }
            catch ( IOException e )
            {
                // TODO: Log this problem somehow?
                e.printStackTrace();
            }
        }
    }
}
