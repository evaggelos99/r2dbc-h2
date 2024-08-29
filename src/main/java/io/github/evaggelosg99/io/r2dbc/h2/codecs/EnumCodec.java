
package io.github.evaggelosg99.io.r2dbc.h2.codecs;

import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.Value;

import io.github.evaggelosg99.io.r2dbc.h2.client.Client;

@SuppressWarnings("rawtypes")
public class EnumCodec extends AbstractCodec<Enum> {

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

		final Object[] ss = value.getDeclaringClass().getEnumConstants();

		final int len = ss.length;

		final String[] enumsStrs = new String[len];

		for (int i = 0; i < len; i++) {
			final Enum en = (Enum) ss[i];

			enumsStrs[i] = en.name();
		}

		return new ExtTypeInfoEnum(enumsStrs).getValue(value.name(), client.getSession());
	}

}