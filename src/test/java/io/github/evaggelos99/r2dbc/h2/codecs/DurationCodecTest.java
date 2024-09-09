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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;

import org.h2.api.IntervalQualifier;
import org.h2.value.Value;
import org.h2.value.ValueInterval;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class DurationCodecTest {

	private DurationCodec codec;

	@BeforeEach
	void setUp() {
		codec = new DurationCodec();
	}

	@Test
	void decode() {
		ValueInterval interval = ValueInterval.from(IntervalQualifier.DAY_TO_SECOND, false, 99_999_999_999_999L,
				24 * 60 * 60 * 1_000_000_000L - 1);
		Duration expected = Duration.ofDays(99_999_999_999_999L).plusHours(23).plusMinutes(59).plusSeconds(59)
				.plusNanos(999_999_999L);

		Duration decoded = codec.decode(interval, Duration.class);
		assertThat(decoded).isEqualTo(expected);
	}

	@Test
	void doCanDecode() {
		assertThat(codec.doCanDecode(Value.INTERVAL_YEAR)).isFalse();
		assertThat(codec.doCanDecode(Value.INTERVAL_MONTH)).isFalse();
		assertThat(codec.doCanDecode(Value.INTERVAL_DAY)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_HOUR)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_MINUTE)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_SECOND)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_YEAR_TO_MONTH)).isFalse();
		assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_HOUR)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_MINUTE)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_DAY_TO_SECOND)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_HOUR_TO_MINUTE)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_HOUR_TO_SECOND)).isTrue();
		assertThat(codec.doCanDecode(Value.INTERVAL_MINUTE_TO_SECOND)).isTrue();
		assertThat(codec.doCanDecode(Value.TIMESTAMP_TZ)).isFalse();
		assertThat(codec.doCanDecode(Value.TIMESTAMP)).isFalse();
		assertThat(codec.doCanDecode(Value.UNKNOWN)).isFalse();
		assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
	}

	@Test
	void doEncode() {
		Duration interval = Duration.ofSeconds(999_999_999_999_999_999L, 999_999_999L);
		ValueInterval expected = ValueInterval.from(IntervalQualifier.SECOND, false, 999_999_999_999_999_999L,
				999_999_999L);
		Value encoded = codec.doEncode(interval);
		assertThat(encoded).isEqualTo(expected);
	}

	@Test
	void doEncodeNoValue() {
		assertThatIllegalArgumentException().isThrownBy(() -> codec.doEncode(null))
				.withMessage("value must not be null");
	}

	@Test
	void encodeNull() {
		assertThat(codec.encodeNull()).isEqualTo(ValueNull.INSTANCE);
	}
}
