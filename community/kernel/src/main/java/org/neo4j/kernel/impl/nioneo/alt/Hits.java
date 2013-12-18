package org.neo4j.kernel.impl.nioneo.alt;

public class Hits
{
    private int hits;

    public void incrementCount( boolean isAllocated)
    {
        if ( !isAllocated )
        {
            hits +=10;
        }
        else
        {
            hits++;
        }
        if ( hits < 0 )
        {
            hits = Integer.MAX_VALUE;
        }
    }

    public int getCount()
    {
        return hits;
    }

    public void decrementCount( boolean isAllocated )
    {
        if ( isAllocated )
        {
            hits = (int) (hits / 1.15f );
        }
        else
        {
            hits = (int) (hits / 1.25f );
        }
    } 
}
