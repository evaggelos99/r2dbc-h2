package io.github.evaggelosg99.io.r2dbc.h2.codecs;

import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import io.github.evaggelosg99.io.r2dbc.h2.util.Assert;

import java.time.LocalTime;

final class LocalTimeCodec extends AbstractCodec<LocalTime> {

    LocalTimeCodec() {
        super(LocalTime.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return dataType == Value.TIME;
    }

    @Override
    LocalTime doDecode(Value value, Class<? extends LocalTime> type) {
        return (LocalTime) JSR310Utils.valueToLocalTime(value, null);
    }

    @Override
    Value doEncode(LocalTime value) {
        return JSR310Utils.localTimeToValue(Assert.requireNonNull(value, "value must not be null"));
    }
}
