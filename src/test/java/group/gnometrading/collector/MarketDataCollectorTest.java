package group.gnometrading.collector;

import com.github.luben.zstd.ZstdInputStream;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MarketDataCollectorTest {

    private static final Listing LISTING = new Listing(532, 151, 499, "id", "id");
    private static final String OUTPUT_BUCKET = "outoutoutout";
    private static final SchemaType TYPE = SchemaType.MBO;

    @Mock
    S3Client s3Client;

    @Mock
    Clock clock;

    @BeforeEach
    void setup() {
        doReturn(ZoneId.of("UTC")).when(clock).getZone();
    }

    @Test
    void testBasicCollector() throws Exception {
        ArgumentCaptor<PutObjectRequest> reqCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
        List<String> uploads = new ArrayList<>();

        doAnswer((Answer<Void>) invocation -> {
            Path path = invocation.getArgument(1);
            uploads.add(parseZstd(path));
            return null;
        }).when(s3Client).putObject(any(PutObjectRequest.class), any(Path.class));

        date(2025, 4, 1, 0, 0);
        MarketDataCollector collector = new MarketDataCollector(clock, s3Client, LISTING, OUTPUT_BUCKET, TYPE);

        date(2025, 4, 1, 1, 0);
        collector.onEvent(buf("1234"), 0, 4);
        date(2025, 4, 1, 2, 0);
        collector.onEvent(buf("4321"), 1, 3);
        date(2025, 4, 1, 2, 30);
        collector.onEvent(buf("hey man"), 0, 7);
        date(2025, 4, 3, 5, 30);
        collector.onEvent(buf("oh no a day"), 0, 11);
        date(2026, 4, 3, 5, 30);
        collector.onEvent(buf("oh no a year"), 0, 11);

        verify(s3Client, times(4)).putObject(reqCaptor.capture(), pathCaptor.capture());

        var allRequests = reqCaptor.getAllValues();

        assertEquals("499/151/2025040100/mbo.zst", allRequests.get(0).key());
        assertEquals(OUTPUT_BUCKET, allRequests.get(0).bucket());
        assertEquals("", uploads.get(0));

        assertEquals("499/151/2025040101/mbo.zst", allRequests.get(1).key());
        assertEquals("1234", uploads.get(1));

        assertEquals("499/151/2025040102/mbo.zst", allRequests.get(2).key());
        assertEquals("321hey man", uploads.get(2));

        assertEquals("499/151/2025040305/mbo.zst", allRequests.get(3).key());
        assertEquals("oh no a day", uploads.get(3));
    }

    private MutableDirectBuffer buf(String input) {
        return new UnsafeBuffer(input.getBytes());
    }

    private String parseZstd(Path path) {
        try (ZstdInputStream zstdStream = new ZstdInputStream(new FileInputStream(path.toFile()))) {
            return new String(zstdStream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void date(int year, int month, int day, int hour, int minute) {
        when(clock.instant()).thenReturn(LocalDateTime.of(year, month, day, hour, minute).toInstant(UTC));
    }

}