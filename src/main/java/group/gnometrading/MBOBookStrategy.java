package group.gnometrading;

import group.gnometrading.books.MBOBook;
import group.gnometrading.schemas.MBODecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

public abstract class MBOBookStrategy {

    private static final int FRAGMENT_LIMIT = 1;

    private final MBOBook book;
    private final MBODecoder mboDecoder;

    public MBOBookStrategy(final MBOBook book) {
        this.book = book;
        this.mboDecoder = new MBODecoder();
    }

    protected abstract void onBookUpdate();
    protected abstract void onOrderUpdate();

    public void onFragment(DirectBuffer buffer, int offset, int length) {
        this.mboDecoder.wrap(buffer, offset, length, MBODecoder.SCHEMA_ID);

        if (this.book.apply(this.mboDecoder)) {
            onBookUpdate();
        } else {
            onOrderUpdate();
        }
    }
}
