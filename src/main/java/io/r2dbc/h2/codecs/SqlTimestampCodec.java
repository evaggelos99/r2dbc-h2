
package io.r2dbc.h2.codecs;

import java.sql.Timestamp;

import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import io.r2dbc.h2.util.Assert;

final class SqlTimestampCodec extends AbstractCodec<Timestamp> {

    SqlTimestampCodec() {
        super(Timestamp.class);
    }

    @Override
    boolean doCanDecode(final int dataType) {
        return dataType == Value.TIME;
    }

    @Override
    Timestamp doDecode(final Value value, final Class<? extends Timestamp> type) {
        return Timestamp.valueOf(JSR310Utils.valueToLocalDateTime(value, null));
    }

    @Override
    Value doEncode(final Timestamp value) {

        return JSR310Utils.localDateTimeToValue(Assert.requireNonNull(value, "value must not be null").toLocalDateTime());
    }

}
