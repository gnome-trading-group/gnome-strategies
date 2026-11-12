package group.gnometrading.strategies.esports;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One venue's two team listings for the session's market, as gnome-launcher's esports_game rule sets them: the same two
 * teams in the same order on every venue.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VenueLegs(
        @JsonProperty("venue") String venue,
        @JsonProperty("listing_team_a") int listingTeamA,
        @JsonProperty("listing_team_b") int listingTeamB) {}
