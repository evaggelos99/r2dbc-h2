package io.r2dbc.h2.codecs;

import static org.assertj.core.api.Assertions.assertThat;

import org.h2.value.Value;
import org.h2.value.ValueEnum;
import org.junit.jupiter.api.Test;

class EnumCodecTest {

	@Test
	void doCanDecode() {
		final EnumCodec codec = new EnumCodec();

		assertThat(codec.doCanDecode(Value.ENUM)).isTrue();
		assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
		assertThat(codec.doCanDecode(Value.VARCHAR)).isFalse();
	}

	@Test
	@SuppressWarnings("unchecked")
	void doDecode() {

		assertThat(new EnumCodec().doDecode(ValueEnum.get("A", 0), TestEnum.class)).isEqualTo(TestEnum.A);
		assertThat(new EnumCodec().doDecode(ValueEnum.get("C", 2), TestEnum.class)).isEqualTo(TestEnum.C);
		assertThat(new EnumCodec().doDecode(ValueEnum.get("C", 5), TestEnum.class)).isEqualTo(TestEnum.C);
		assertThat(new EnumCodec().doDecode(ValueEnum.get("A", 5), TestEnum.class)).isNotEqualTo(TestEnum.C);
	}

	@Test
	void doEncode() {

		assertThat(new EnumCodec().doEncode(TestEnum.B)).isEqualTo((ValueEnum.get("B", 1)));
		assertThat(new EnumCodec().doEncode(TestEnum.A)).isNotEqualTo((ValueEnum.get("B", 1)));
	}

	private enum TestEnum {

		A, B, C
	}

}
