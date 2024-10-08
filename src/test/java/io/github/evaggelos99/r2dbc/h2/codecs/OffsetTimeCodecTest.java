package io.github.evaggelos99.r2dbc.h2.codecs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetTime;
import java.time.ZoneOffset;

import org.h2.engine.Session;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueTimeTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.evaggelos99.r2dbc.h2.client.Client;

final class OffsetTimeCodecTest {

	private Client client;

	@BeforeEach
	void setUp() {
		this.client = mock(Client.class);

		when(this.client.getSession()).thenReturn(mock(Session.class));
	}

	@Test
	void decode() {
		assertThat(new OffsetTimeCodec(client).decode(ValueTimeTimeZone.parse("23:59:59.999999999Z"), OffsetTime.class))
				.isEqualTo(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.UTC));
		assertThat(new OffsetTimeCodec(client).decode(ValueTimeTimeZone.parse("10:20:30+02"), OffsetTime.class))
				.isEqualTo(OffsetTime.of(10, 20, 30, 0, ZoneOffset.ofHours(2)));
	}

	@Test
	void doCanDecode() {
		OffsetTimeCodec codec = new OffsetTimeCodec(client);

		assertThat(codec.doCanDecode(Value.TIME_TZ)).isTrue();
		assertThat(codec.doCanDecode(Value.TIME)).isFalse();
		assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
		assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
	}

	@Test
	void doEncode() {
		assertThat(new OffsetTimeCodec(client).doEncode(OffsetTime.of(23, 59, 59, 999999999, ZoneOffset.UTC)))
				.isEqualTo(ValueTimeTimeZone.parse("23:59:59.999999999Z"));
		assertThat(new OffsetTimeCodec(client).doEncode(OffsetTime.of(10, 20, 30, 0, ZoneOffset.ofHours(2))))
				.isEqualTo(ValueTimeTimeZone.parse("10:20:30+02"));
	}

	@Test
	void doEncodeNoValue() {
		assertThatIllegalArgumentException().isThrownBy(() -> new OffsetTimeCodec(client).doEncode(null))
				.withMessage("value must not be null");
	}

	@Test
	void encodeNull() {
		assertThat(new OffsetTimeCodec(client).encodeNull()).isEqualTo(ValueNull.INSTANCE);
	}
}
