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

import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

final class LongCodecTest {

	@Test
	void decode() {
		assertThat(new LongCodec().decode(ValueBigint.get(100), Long.class)).isEqualTo(100);
	}

	@Test
	void doCanDecode() {
		LongCodec codec = new LongCodec();

		assertThat(codec.doCanDecode(Value.BIGINT)).isTrue();
		assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
	}

	@Test
	void doEncode() {
		assertThat(new LongCodec().doEncode(100L)).isEqualTo(ValueBigint.get(100));
	}

	@Test
	void doEncodeNoValue() {
		assertThatIllegalArgumentException().isThrownBy(() -> new LongCodec().doEncode(null))
				.withMessage("value must not be null");
	}

	@Test
	void encodeNull() {
		assertThat(new LongCodec().encodeNull()).isEqualTo(ValueNull.INSTANCE);
	}
}
