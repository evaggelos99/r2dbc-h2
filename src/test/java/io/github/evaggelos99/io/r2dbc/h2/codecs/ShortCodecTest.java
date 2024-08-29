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

package io.github.evaggelos99.io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueSmallint;
import org.junit.jupiter.api.Test;

import io.github.evaggelos99.io.r2dbc.h2.codecs.ShortCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class ShortCodecTest {

    @Test
    void decode() {
        assertThat(new ShortCodec().decode(ValueSmallint.get((short) 100), Short.class))
            .isEqualTo((short) 100);
    }

    @Test
    void doCanDecode() {
        ShortCodec codec = new ShortCodec();

        assertThat(codec.doCanDecode(Value.SMALLINT)).isTrue();
        assertThat(codec.doCanDecode(Value.INTEGER)).isFalse();
    }

    @Test
    void doEncode() {
        assertThat(new ShortCodec().doEncode((short) 100))
            .isEqualTo(ValueSmallint.get((short) 100));
    }

    @Test
    void doEncodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ShortCodec().doEncode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        assertThat(new ShortCodec().encodeNull())
            .isEqualTo(ValueNull.INSTANCE);
    }
}
