
package io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueEnumBase;

@SuppressWarnings("rawtypes")
public class EnumCodec extends AbstractCodec<Enum> {

	EnumCodec() {
		super(Enum.class);
	}

	@Override
	boolean doCanDecode(final int dataType) {

		return Value.ENUM == dataType;
	}

	@SuppressWarnings("unchecked")
	@Override
	Enum doDecode(final Value value, final Class<? extends Enum> type) {

		final ValueEnumBase valueEnumBase = (ValueEnumBase) value;
		return Enum.valueOf(type, valueEnumBase.getString());
	}

	@Override
	Value doEncode(final Enum value) {

		return ValueEnumBase.get(value.name(), value.ordinal());
	}

}