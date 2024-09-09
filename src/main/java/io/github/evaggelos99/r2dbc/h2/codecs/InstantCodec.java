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

import java.time.Instant;

import org.h2.engine.CastDataProvider;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;

import io.github.evaggelos99.r2dbc.h2.client.Client;
import io.github.evaggelos99.r2dbc.h2.util.Assert;

/**
 * Codec responsible for Instants
 */
public class InstantCodec extends AbstractCodec<Instant> {

	private final Client client;

	/**
	 * C-or
	 *
	 * @param client
	 */
	public InstantCodec(final Client client) {
		super(Instant.class);
		this.client = client;
	}

	@Override
	boolean doCanDecode(final int dataType) {
		return dataType == Value.TIMESTAMP_TZ;
	}

	@Override
	Instant doDecode(final Value value, final Class<? extends Instant> type) {
		Assert.requireType(this.client.getSession(), CastDataProvider.class,
				"The session must implement CastDataProvider.");
		return JSR310Utils.valueToInstant(value, this.client.getSession());
	}

	@Override
	Value doEncode(final Instant value) {
		return JSR310Utils.instantToValue(Assert.requireNonNull(value, "value must not be null"));
	}
}
