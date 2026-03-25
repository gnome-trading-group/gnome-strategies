package group.gnometrading;

import group.gnometrading.books.MboBook;
import group.gnometrading.schemas.MboDecoder;
import org.agrona.DirectBuffer;

public abstract class MboBookStrategy {

    private static final int FRAGMENT_LIMIT = 1;

    private final MboBook book;
    private final MboDecoder mboDecoder;

    public MboBookStrategy(final MboBook book) {
        this.book = book;
        this.mboDecoder = new MboDecoder();
    }

    protected abstract void onBookUpdate();

    protected abstract void onOrderUpdate();

    public final void onFragment(DirectBuffer buffer, int offset, int length) {
        this.mboDecoder.wrap(buffer, offset, length, MboDecoder.SCHEMA_ID);

        if (this.book.apply(this.mboDecoder)) {
            onBookUpdate();
        } else {
            onOrderUpdate();
        }
    }
}
