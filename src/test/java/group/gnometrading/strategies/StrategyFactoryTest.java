package group.gnometrading.strategies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.SecurityMaster;
import group.gnometrading.oms.position.PositionView;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sequencer.SequencedRingBuffer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StrategyFactoryTest {

    /** A strategy with arguments of several types after the infrastructure. */
    public static final class ConfiguredStrategy extends StrategyAgent {
        final int strategyId;
        final int depth;
        final double threshold;
        final String name;
        final List<Integer> levels;

        public ConfiguredStrategy(
                int strategyId,
                SequencedRingBuffer<?> marketDataBuffer,
                SequencedRingBuffer<OrderExecutionReport> execReportBuffer,
                SequencedRingBuffer<Intent> intentBuffer,
                PositionView positionView,
                SecurityMaster securityMaster,
                int depth,
                double threshold,
                String name,
                List<Integer> levels) {
            super(strategyId, marketDataBuffer, execReportBuffer, intentBuffer, positionView, securityMaster);
            this.strategyId = strategyId;
            this.depth = depth;
            this.threshold = threshold;
            this.name = name;
            this.levels = levels;
        }

        @Override
        protected void onMarketData(Schema data) {}

        @Override
        protected void onExecutionReport(OrderExecutionReport report) {}
    }

    @Test
    void argumentsAreMatchedByNameAndConverted() {
        StrategyAgent agent = StrategyFactory.createWithOwnBuffers(
                ConfiguredStrategy.class.getName(),
                12,
                null,
                null,
                Map.of("depth", 3L, "threshold", 2, "name", "mm", "levels", List.of(1, 2)));

        ConfiguredStrategy strategy = (ConfiguredStrategy) agent;
        assertEquals(12, strategy.strategyId);
        assertEquals(3, strategy.depth);
        assertEquals(2.0, strategy.threshold);
        assertEquals("mm", strategy.name);
        assertEquals(List.of(1, 2), strategy.levels);
    }

    @Test
    void argumentsThatMatchNoConstructorAreAClearError() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> StrategyFactory.createWithOwnBuffers(
                        ConfiguredStrategy.class.getName(), 1, null, null, Map.of("depth", 3)));
        assertTrue(error.getMessage().contains("[depth]"), error.getMessage());
    }

    @Test
    void unknownClassIsAClearError() {
        assertThrows(
                IllegalArgumentException.class,
                () -> StrategyFactory.createWithOwnBuffers("no.such.Strategy", 1, null, null, Map.of()));
    }
}
