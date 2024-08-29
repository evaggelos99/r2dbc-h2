/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.evaggelos99.io.r2dbc.h2.codecs;

import org.h2.api.Interval;
import org.h2.value.Value;
import org.h2.value.ValueInterval;

import io.github.evaggelos99.io.r2dbc.h2.util.Assert;

final class IntervalCodec extends AbstractCodec<Interval> {

    IntervalCodec() {
        super(Interval.class);
    }

    @Override
    boolean doCanDecode(int dataType) {
        return Value.INTERVAL_YEAR <= dataType && dataType <= Value.INTERVAL_MINUTE_TO_SECOND;
    }

    @Override
    Interval doDecode(Value value, Class<? extends Interval> type) {
        ValueInterval valueInterval = (ValueInterval) value;
        return new Interval(valueInterval.getQualifier(), valueInterval.isNegative(), valueInterval.getLeading(), valueInterval.getRemaining());
    }

    @Override
    Value doEncode(Interval value) {
        Assert.requireNonNull(value, "value must not be null");
        return ValueInterval.from(value.getQualifier(), value.isNegative(), value.getLeading(), value.getRemaining());
    }
}
