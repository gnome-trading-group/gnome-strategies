package group.gnometrading.strategies.esports;

import group.gnometrading.SecurityMaster;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.ListingSpec;
import group.gnometrading.sm.fee.FeeTimeline;

/**
 * The registry facts the strategy reads about its listings. Specs are read on every decision, so an implementation
 * answers from memory without allocating; the security master does, once a listing's history is cached.
 */
interface ListingFacts {

    Listing listing(int listingId);

    FeeTimeline feeTimeline(int listingId);

    /** The listing's spec in force at {@code epochNanos}, or null if it had none. */
    ListingSpec spec(int listingId, long epochNanos);

    static ListingFacts of(SecurityMaster securityMaster) {
        return new ListingFacts() {
            @Override
            public Listing listing(int listingId) {
                return securityMaster.getListing(listingId);
            }

            @Override
            public FeeTimeline feeTimeline(int listingId) {
                return securityMaster.getFeeTimeline(listingId);
            }

            @Override
            public ListingSpec spec(int listingId, long epochNanos) {
                return securityMaster.getListingSpec(listingId, epochNanos);
            }
        };
    }
}
