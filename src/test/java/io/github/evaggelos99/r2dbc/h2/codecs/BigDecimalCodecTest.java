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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.math.BigDecimal;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.junit.jupiter.api.Test;

final class BigDecimalCodecTest {

	private final BigDecimal BIG = new BigDecimal(Integer.MAX_VALUE).add(new BigDecimal(Integer.MAX_VALUE));

	@Test
	void decode() {
		assertThat(new BigDecimalCodec().decode(ValueNumeric.get(BIG), BigDecimal.class)).isEqualTo(BIG);
	}

	@Test
	void doCanDecode() {
		final BigDecimalCodec codec = new BigDecimalCodec();

		assertThat(codec.doCanDecode(Value.NUMERIC)).isTrue();
		assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
		assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
	}

	@Test
	void doEncode() {
		assertThat(new BigDecimalCodec().doEncode(BIG)).isEqualTo(ValueNumeric.get(BIG));
	}

	@Test
	void doEncodeNoValue() {
		assertThatIllegalArgumentException().isThrownBy(() -> new BigDecimalCodec().doEncode(null))
				.withMessage("value must not be null");
	}

	@Test
	void encodeNull() {
		assertThat(new BigDecimalCodec().encodeNull()).isEqualTo(ValueNull.INSTANCE);
	}
}
