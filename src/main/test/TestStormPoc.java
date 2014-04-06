import com.google.gson.Gson;
import com.kiku.gridlattice.LocationChangedEvent;
import junit.framework.Assert;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by rajan on 06/04/2014.
 */

public class TestStormPoc {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestStormPoc.class);
    String jsonEvent = "{\"eventType\": \"ENTER\", \"userId\": \"Robert_9\", \"time\": 1374916043917, \"roomId\": \"Cafetaria\", \"id\": \"5ff7776b8b54de2c64839699b88ec229A\", \"corrId\": \"5ff7776b8b54de2c64839699b88ec229\"}";

    static LocationChangedEvent newEvent = new LocationChangedEvent();

    @BeforeClass
    public static void beforeClass() {

        newEvent.setEventType(LocationChangedEvent.EVENT_TYPE.ENTER);
        newEvent.setUserId("Rajan");
        newEvent.setTime(DateTime.now().getMillis());
        newEvent.setRoomId("Kitchen");
        newEvent.setId(UUID.randomUUID().toString());
        newEvent.setCorrId(UUID.randomUUID().toString());

    }

    @Test
    public void testJackson() throws IOException {

        // Jackson
        ObjectMapper mapper = new ObjectMapper();
        LocationChangedEvent event = mapper.readValue(jsonEvent, LocationChangedEvent.class);
        Assert.assertNotNull(event);
        Assert.assertEquals(LocationChangedEvent.EVENT_TYPE.ENTER, event.getEventType());
        Assert.assertEquals("Robert_9", event.getUserId());

        for(int i=0;i<1000;i++) {
            String jacksonStr = mapper.writeValueAsString(newEvent);
            Assert.assertNotNull(jacksonStr);
        }

    }

    @Test
    public void testGson() throws IOException {

        // GSON
        Gson gson = new Gson();
        LocationChangedEvent lc =  gson.fromJson(jsonEvent, LocationChangedEvent.class);
        Assert.assertNotNull(lc);
        Assert.assertEquals(LocationChangedEvent.EVENT_TYPE.ENTER, lc.getEventType());
        Assert.assertEquals("Robert_9", lc.getUserId());

        for(int i=0;i<1000;i++) {
            String gsonStr = gson.toJson(newEvent);
            Assert.assertNotNull(gsonStr);
        }
    }
    @Test
    public void testOpen() {

        int x = 2;
        Assert.assertEquals ("Does not equal 2",2,x);
    }
}
