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

package io.github.evaggelos99.r2dbc.h2.codecs;

import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import io.github.evaggelos99.r2dbc.h2.util.Assert;

import java.time.Period;

final class PeriodCodec extends AbstractCodec<Period> {

	PeriodCodec() {
		super(Period.class);
	}

	@Override
	boolean doCanDecode(int dataType) {
		return dataType == Value.INTERVAL_YEAR || dataType == Value.INTERVAL_MONTH
				|| dataType == Value.INTERVAL_YEAR_TO_MONTH;
	}

	@Override
	Period doDecode(Value value, Class<? extends Period> type) {
		return (Period) JSR310Utils.valueToPeriod(value);
	}

	@Override
	Value doEncode(Period value) {
		Assert.requireNonNull(value, "value must not be null");
		return JSR310Utils.periodToValue(value);
	}
}
