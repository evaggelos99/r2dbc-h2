package io.github.evaggelos99.r2dbc.h2.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.h2.engine.Session;
import org.h2.value.ExtTypeInfoEnum;
import org.h2.value.Value;
import org.h2.value.ValueEnum;
import org.junit.jupiter.api.Test;

import io.github.evaggelos99.r2dbc.h2.client.Client;
import io.github.evaggelos99.r2dbc.h2.codecs.EnumCodec;

class EnumCodecTest {

	private Client client;

	@Test
	void doCanDecode() {

		this.client = mock(Client.class);

		final EnumCodec codec = new EnumCodec(client);

		assertThat(codec.doCanDecode(Value.ENUM)).isTrue();
		assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
		assertThat(codec.doCanDecode(Value.VARCHAR)).isFalse();
	}

	@Test
	@SuppressWarnings("unchecked")
	void doDecode() {

		this.client = mock(Client.class);

		assertThat(new EnumCodec(client).doDecode(ValueEnum.get("A", 0), TestEnum.class)).isEqualTo(TestEnum.A);
		assertThat(new EnumCodec(client).doDecode(ValueEnum.get("C", 2), TestEnum.class)).isEqualTo(TestEnum.C);
		assertThat(new EnumCodec(client).doDecode(ValueEnum.get("C", 5), TestEnum.class)).isEqualTo(TestEnum.C);
		assertThat(new EnumCodec(client).doDecode(ValueEnum.get("A", 5), TestEnum.class)).isNotEqualTo(TestEnum.C);
	}

	@Test
	void doEncode() {

		this.client = mock(Client.class);
		final Session sessionMock = mock(Session.class);
		when(this.client.getSession()).thenReturn(sessionMock);
		when(sessionMock.zeroBasedEnums()).thenReturn(false);

		final ExtTypeInfoEnum expectedEnums = new ExtTypeInfoEnum(new String[] { "A", "B", "C" });

		assertThat(new EnumCodec(client).doEncode(TestEnum.A)).isNotEqualTo(expectedEnums.getValue("B", null));
		assertThat(new EnumCodec(client).doEncode(TestEnum.B)).isEqualTo(expectedEnums.getValue("B", null));
	}

	private enum TestEnum {

		A, B, C
	}

}
