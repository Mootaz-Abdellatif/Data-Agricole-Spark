package fr.troisil.info;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class MainStreamingIT
{

    @Test
    public void runMain() throws InterruptedException {
        // Thread
        MainStreaming.main(new String[]{});
        assertTrue( true );
    }
}
