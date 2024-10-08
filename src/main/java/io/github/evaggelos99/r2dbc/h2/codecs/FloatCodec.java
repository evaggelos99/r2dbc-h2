/*
 * Copyright 2018 the original author or authors.
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

import org.h2.value.Value;
import org.h2.value.ValueReal;

import io.github.evaggelos99.r2dbc.h2.util.Assert;

final class FloatCodec extends AbstractCodec<Float> {

	FloatCodec() {
		super(Float.class);
	}

	@Override
	boolean doCanDecode(int dataType) {
		return dataType == Value.REAL;
	}

	@Override
	Float doDecode(Value value, Class<? extends Float> type) {
		return value.getFloat();
	}

	@Override
	Value doEncode(Float value) {
		return ValueReal.get(Assert.requireNonNull(value, "value must not be null"));
	}
}
