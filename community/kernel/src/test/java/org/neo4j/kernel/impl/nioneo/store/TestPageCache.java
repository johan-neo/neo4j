package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.alt.FileWithRecords;
import org.neo4j.kernel.impl.nioneo.alt.HitStat;
import org.neo4j.kernel.impl.nioneo.alt.PageSynchronization;
import org.neo4j.kernel.impl.nioneo.alt.PageType;
import org.neo4j.kernel.impl.nioneo.alt.PagedFileWithRecords;
import org.neo4j.kernel.impl.nioneo.alt.RecordStore;
import org.neo4j.kernel.impl.nioneo.alt.Sweeper;

public class TestPageCache
{
    private final Random r = new Random( System.currentTimeMillis() );
    private long fileSize;
    private int recordSize;
    private int nrOfRecords;
    private File file;
    private RandomAccessFile rf;
    private FileChannel channel;
    private RecordStore store;

    @Before
    public void setupStore() throws IOException
    {
        int targetPageSize = ( 128 + r.nextInt( 4*1024 ) );
        recordSize = 8 + r.nextInt( 120 );
        fileSize = (long) Math.pow( 4, r.nextInt( 8 ) ) * 1024;
        
        file = new File( "test-page-cache" );
        rf = new RandomAccessFile( file, "rw" );
        channel = rf.getChannel();
        channel.truncate( fileSize );
        FileWithRecords fwr = new FileWithRecords( "test-page-cache", channel, recordSize );
        nrOfRecords = (int) fwr.getNrOfRecords();
        store = new PagedFileWithRecords( fwr, targetPageSize, PageType.MEMORY_MAPPED, PageSynchronization.ATOMIC );
        
        store.sweep( new Sweeper() {

            @Override
            public long getMemoryDelta( HitStat newStat )
            {
                return Long.MAX_VALUE;
            }
        }  );
   
        byte[] data = new byte[recordSize];
        int nrOfShorts = recordSize / 2;
        int valueToRandomizeFrom = 65525 / 2 - nrOfShorts - 1;
        for ( int i = 0; i < fwr.getNrOfRecords(); i++ )
        {
            ByteBuffer buffer = ByteBuffer.wrap( data );
            short value = (short) r.nextInt( valueToRandomizeFrom );
            for ( int j = 0; j < nrOfShorts; j++ )
            {
                buffer.putShort( value++ );
            }
            store.writeRecord( i, data );
        }
    }
    
    @After
    public void closeStore() throws IOException
    {
        store.close();
        channel.close();
        rf.close();
        file.delete();
    }
    
    @Test
    public void testConcurrencySingleRecord() throws IOException
    {
        int nrOfWriters = 2;
        int nrOfReaders = 4;
        List<WorkerThread> threads = new ArrayList<WorkerThread>();
        for ( int i = 0; i < nrOfWriters; i++ )
        {
            WriterThread thread = new WriterThread( store, 1 );
            threads.add( thread );
        }
        for ( int i = 0; i < nrOfReaders; i++ )
        {
            ReaderThread thread = new ReaderThread( store, 1 );
            threads.add( thread );
        }
        
        for ( WorkerThread thread : threads )
        {
            thread.start();
        }
        long timeToRun = 10*1000;
        long start = System.currentTimeMillis();
        do
        {
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        } while ( System.currentTimeMillis() - start < timeToRun );
        for ( WorkerThread thread : threads )
        {
            thread.stopRunning();
        }
        for ( WorkerThread thread : threads )
        {
            try
            {
                thread.join();
                assertEquals( 0, thread.getNrOfErrors() );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }
    
    /**
     * Randomly writes increasing short (16 bit signed) values to each record while 
     * concurrently randomly reading records making sure each record contains a 
     * correct sequence of shorts.
     * 
     * @throws IOException
     */
    @Test
    public void testConcurrencyRandom() throws IOException
    {
        int nrOfCores = Runtime.getRuntime().availableProcessors();
        int nrOfThreads = nrOfCores + r.nextInt( nrOfCores );
        WorkerThread[] threads = new WorkerThread[nrOfThreads];
        for ( int i = 0; i < threads.length; i++ )
        {
            threads[i] = new ReadWriteThread( store, nrOfRecords );
        }
        for ( WorkerThread thread : threads )
        {
            thread.start();
        }
        long timeToRun = 20*1000;
        long start = System.currentTimeMillis();
        do
        {
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        } while ( System.currentTimeMillis() - start < timeToRun );
        for ( WorkerThread thread : threads )
        {
            thread.stopRunning();
        }
        for ( WorkerThread thread : threads )
        {
            try
            {
                thread.join();
                assertEquals( 0, thread.getNrOfErrors() );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
    }
    
    static class WriterThread extends WorkerThread
    {
        public WriterThread( RecordStore store, int nrOfRecords )
        {
            super( store, nrOfRecords );
        }

        @Override
        public void run()
        {
            byte[] data = new byte[getStore().getRecordSize()];
            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( doRun() )
            {
                writeRandomRecord( data, buffer );
            }
        }
    }
    
    static class ReaderThread extends WorkerThread
    {
        public ReaderThread( RecordStore store, int nrOfRecords )
        {
            super( store, nrOfRecords );
        }

        @Override
        public void run()
        {
            byte[] data = new byte[getStore().getRecordSize()];
            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( doRun() )
            {
                readAndAssertRandomRecord( data, buffer );
            }
        }
    }
    
    static class ReadWriteThread extends WorkerThread
    {
        private final int isWrite;
        
        public ReadWriteThread( RecordStore store, int nrOfRecords )
        {
            super( store, nrOfRecords );
            
            this.isWrite = nrOfRecords / 9;
        }

        @Override
        public void run()
        {
            byte[] data = new byte[getStore().getRecordSize()];
            ByteBuffer buffer = ByteBuffer.wrap( data );
            while ( doRun() )
            {
                if ( getRandom() < isWrite )
                {
                    writeRandomRecord( data, buffer );
                }
                else
                {
                    readAndAssertRandomRecord( data, buffer );
                }
            }
        }
    }
    
    abstract static class WorkerThread extends Thread
    {
        private final RecordStore store;
        private final int[] randomRecords = new int[200000];
        private int randomPos = 0;
        private final Random slowRandom = new Random( System.currentTimeMillis() );
        
        private boolean doRun = true;
        private final int nrOfRecords; 
        
        private AtomicInteger nrOfErrors = new AtomicInteger( 0 );
        
        public WorkerThread( RecordStore store, int nrOfRecords )
        {
            this.store = store;
            this.nrOfRecords = nrOfRecords;
            Random r = new Random( System.currentTimeMillis() );
            
            for ( int i = 0; i < randomRecords.length; i++ )
            {
                randomRecords[i] = r.nextInt( nrOfRecords );
            }
        }

        protected void writeRandomRecord( byte[] data, ByteBuffer buffer )
        {
            int record = getRandom();
            buffer.clear();
            int nrOfShorts = buffer.capacity() / 2;
            short value = (short) getSlowRandom().nextInt( 10 ); // (short) getSlowRandom().nextInt( 65535 / 2 - nrOfShorts - 1 );
            for ( int i = 0; i < nrOfShorts; i++ )
            {
                buffer.putShort( value++ );
            }
            getStore().writeRecord( record, data );
        }
        
        protected void readAndAssertRandomRecord( byte[] data, ByteBuffer buffer )
        {
            int record = getRandom();
            buffer.clear();
            getStore().getRecord( record, data );
            int value = buffer.getShort();
            while ( buffer.remaining() >= 2 )
            {
                if ( value + 1 != buffer.getShort() )
                {
                    addError();
                    System.out.print( buffer.capacity() + " " + 
                            buffer.position() + " " + buffer.remaining() + ": " );
                    buffer.clear();
                    for ( int i = 0; i < buffer.capacity() / 2; i++ )
                    {
                        System.out.print( buffer.getShort() + " " );
                    }
                    System.out.println();
                    break;
                }
                value++;
            }
        }
        
        public int getNrOfErrors()
        {
            return nrOfErrors.get();
        }
        
        public void addError()
        {
            nrOfErrors.incrementAndGet();
        }

        public int getRandom()
        {
            int value = randomRecords[randomPos++];
            if ( randomPos >= randomRecords.length )
            {
                randomPos = 0;
            }
            return value;
        }
        
        public Random getSlowRandom()
        {
            return slowRandom;
        }
        
        public RecordStore getStore()
        {
            return store;
        }
        
        public boolean doRun()
        {
            return doRun;
        }
        
        public void stopRunning()
        {
            doRun = false;
        }
    }
}
