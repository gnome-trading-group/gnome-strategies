package group.gnometrading;

import group.gnometrading.books.MBOBook;
import group.gnometrading.objects.MarketUpdateDecoder;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

public abstract class MBOBookStrategy implements Agent, FragmentHandler {

    private static final int FRAGMENT_LIMIT = 1;

    private final MBOBook book;
    private final Subscription subscription;
    private final MarketUpdateDecoder marketUpdateDecoder;

    public MBOBookStrategy(final MBOBook book, final Subscription subscription) {
        this.book = book;
        this.subscription = subscription;
        this.marketUpdateDecoder = new MarketUpdateDecoder();
    }

    @Override
    public int doWork() throws Exception {
        // TODO: Check if the subscription is connected?
        subscription.poll(this, FRAGMENT_LIMIT);
        return 0;
    }

    protected abstract void onBookUpdate();
    protected abstract void onOrderUpdate();

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        this.marketUpdateDecoder.wrap(buffer, offset, length, MarketUpdateDecoder.SCHEMA_ID);

        if (this.book.apply(this.marketUpdateDecoder)) {
            onBookUpdate();
        } else {
            onOrderUpdate();
        }
    }
}
