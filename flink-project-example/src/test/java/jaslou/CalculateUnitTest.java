package jaslou;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CalculateUnitTest.class)
public class CalculateUnitTest {

    @Test
    public void add(){
        PowerMockito.mockStatic(CalculateUnitTest.class);
        Mockito.when(PowermockTest.add(1, 1));//.thenReturn(1);
        assertEquals(0,PowermockTest.add(1,1));
    }
}
