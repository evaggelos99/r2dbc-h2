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

import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.Value;

import io.github.evaggelos99.r2dbc.h2.client.Client;

/**
 * Codec responsible for Enums
 */
@SuppressWarnings("rawtypes")
public final class EnumCodec extends AbstractCodec<Enum> {

	private final Client client;

	EnumCodec(final Client client) {
		super(Enum.class);
		this.client = client;
	}

	@Override
	boolean doCanDecode(final int dataType) {

		return Value.ENUM == dataType;
	}

	@SuppressWarnings("unchecked")
	@Override
	Enum doDecode(final Value value, final Class<? extends Enum> type) {

		return Enum.valueOf(type, value.getString());
	}

	@Override
	Value doEncode(final Enum value) {

		final Object[] enumConstants = value.getDeclaringClass().getEnumConstants();

		final int length = enumConstants.length;

		final String[] enumStrings = new String[length];

		for (int i = 0; i < length; i++) {
			final Enum en = (Enum) enumConstants[i];

			enumStrings[i] = en.name();
		}

		return new ExtTypeInfoEnum(enumStrings).getValue(value.name(), client.getSession());
	}

}