package group.gnometrading;

import group.gnometrading.books.MBOBook;
import group.gnometrading.schemas.MBODecoder;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

public abstract class MBOBookStrategy implements Agent, FragmentHandler {

    private static final int FRAGMENT_LIMIT = 1;

    private final MBOBook book;
    private final Subscription subscription;
    private final MBODecoder mboDecoder;

    public MBOBookStrategy(final MBOBook book, final Subscription subscription) {
        this.book = book;
        this.subscription = subscription;
        this.mboDecoder = new MBODecoder();
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
        this.mboDecoder.wrap(buffer, offset, length, MBODecoder.SCHEMA_ID);

        if (this.book.apply(this.mboDecoder)) {
            onBookUpdate();
        } else {
            onOrderUpdate();
        }
    }
}
