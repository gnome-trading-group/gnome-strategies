package group.gnometrading.collector;

import group.gnometrading.collector.merger.MarketDataMerger;
import group.gnometrading.collector.transformer.MarketDataTransformer;
import group.gnometrading.logging.LogMessage;
import group.gnometrading.logging.Logger;
import group.gnometrading.sm.Listing;

import java.util.Set;

/**
 * This class is invoked via an AWS Lambda. It first takes the raw files
 * in the S3 bucket produced by a MarketDataCollector and merges them into a
 * consolidated stream. It then takes the consolidated stream and transforms it
 * into lower-level schemas.
 */
public class MarketDataAggregator {

    private final Logger logger;
    private final MarketDataMerger marketDataMerger;
    private final MarketDataTransformer marketDataTransformer;

    public MarketDataAggregator(
            Logger logger,
            MarketDataMerger marketDataMerger,
            MarketDataTransformer marketDataTransformer
    ) {
        this.logger = logger;
        this.marketDataMerger = marketDataMerger;
        this.marketDataTransformer = marketDataTransformer;
    }

    public void run() {
        logger.logf(LogMessage.DEBUG, "Running market data aggregator");
        Set<Listing> mergedKeys = this.marketDataMerger.runMerger();
        this.marketDataTransformer.runTransformer(mergedKeys);
    }

}
