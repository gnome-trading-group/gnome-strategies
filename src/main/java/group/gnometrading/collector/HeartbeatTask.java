package group.gnometrading.collector;

import group.gnometrading.networking.http.HTTPClient;
import group.gnometrading.networking.http.HTTPProtocol;
import group.gnometrading.networking.http.HTTPResponse;
import group.gnometrading.sm.Listing;
import org.agrona.concurrent.Agent;

import java.io.IOException;

public class HeartbeatTask implements Agent {

    private static final String API_KEY_HEADER = "x-api-key";
    private static final String HEARTBEAT_PATH = "/api/collectors/heartbeat";

    private final String controllerUrl;
    private final String controllerApiKey;
    private final byte[] body;
    private final HTTPClient httpClient;

    public HeartbeatTask(
            final String controllerUrl,
            final String controllerApiKey,
            final Listing listing
    ) {
        this.controllerUrl = controllerUrl;
        this.controllerApiKey = controllerApiKey;
        this.body = this.buildBody(listing);
        this.httpClient = new HTTPClient();
    }

    private byte[] buildBody(final Listing listing) {
        return """
               {"listingId": %d}
              """.formatted(listing.listingId()).trim().getBytes();
    }

    @Override
    public int doWork() {
        try {
            final HTTPResponse response = httpClient.post(
                    HTTPProtocol.HTTPS,
                    this.controllerUrl,
                    HEARTBEAT_PATH,
                    this.body,
                    API_KEY_HEADER,
                    this.controllerApiKey
            );

            if (!response.isSuccess()) {
                throw new RuntimeException("Unable to send heartbeat. Status code: " + response.getStatusCode());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    @Override
    public String roleName() {
        return "heartbeat";
    }
}
