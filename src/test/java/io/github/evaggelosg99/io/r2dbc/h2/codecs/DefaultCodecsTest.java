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

package io.github.evaggelosg99.io.r2dbc.h2.codecs;

import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;
import org.junit.jupiter.api.Test;

import io.github.evaggelosg99.io.r2dbc.h2.client.Client;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.ArrayCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.BigDecimalCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.BlobCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.BlobToByteBufferCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.BooleanCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.ByteCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.BytesCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.ClobCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.Codec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.DefaultCodecs;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.DoubleCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.FloatCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.GeometryCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.InstantCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.IntegerCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.JsonCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.LocalDateCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.LocalDateTimeCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.LocalTimeCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.LongCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.ShortCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.StringCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.UuidCodec;
import io.github.evaggelosg99.io.r2dbc.h2.codecs.ZonedDateTimeCodec;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.*;

final class DefaultCodecsTest {

    @Test
    void addOptionalCodecsGeometry() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willReturn(Object.class)
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));

        Codec<?> result = DefaultCodecs.addOptionalCodecs(mockClassLoader)
            .findFirst()
            .get();

        assertThat(result).isExactlyInstanceOf(GeometryCodec.class);
    }

    @Test
    void addOptionalCodecsGeometryNotFound() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willThrow(new ClassNotFoundException())
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));

        long result = DefaultCodecs.addOptionalCodecs(mockClassLoader).count();

        assertThat(result).isEqualTo(0L);
    }

    @Test
    void canDecodeGeometry() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willReturn(Object.class)
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));
        ValueGeometry value = ValueGeometry.get("POINT(1 2)");

        DefaultCodecs defaultCodecs = new DefaultCodecs(mock(Client.class));

        assertThat(defaultCodecs.decode(value, value.getValueType(), Object.class)).isEqualTo(value.getGeometry());
    }

    @Test
    void createCodecsWithNonOptionalCodecsAndNoDuplicates() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willThrow(new ClassNotFoundException())
            .given(mockClassLoader)
            .loadClass(any());

        Stream<Class<?>> result = DefaultCodecs.createCodecs(mock(Client.class), mockClassLoader, null)
            .stream()
            .map(Codec::getClass);

        assertThat(result).containsOnlyOnce(
            BigDecimalCodec.class,
            BlobToByteBufferCodec.class,
            BlobCodec.class,
            BooleanCodec.class,
            ByteCodec.class,
            BytesCodec.class,
            ClobCodec.class,
            DoubleCodec.class,
            FloatCodec.class,
            IntegerCodec.class,
            JsonCodec.class,
            LocalDateCodec.class,
            LocalDateTimeCodec.class,
            LocalTimeCodec.class,
            LongCodec.class,
            ShortCodec.class,
            StringCodec.class,
            UuidCodec.class,
            ZonedDateTimeCodec.class,
            InstantCodec.class,
            ArrayCodec.class
        );
    }

    @Test
    void createCodecsWithOptionalCodecsAndNoDuplicates() throws Exception {
        ClassLoader mockClassLoader = mock(ClassLoader.class);
        willReturn(Object.class)
            .given(mockClassLoader)
            .loadClass(eq("org.locationtech.jts.geom.Geometry"));

        Stream<Class<?>> result = DefaultCodecs.createCodecs(mock(Client.class), mockClassLoader, null)
            .stream()
            .map(Codec::getClass);

        assertThat(result).containsOnlyOnce(
            BigDecimalCodec.class,
            BlobToByteBufferCodec.class,
            BlobCodec.class,
            BooleanCodec.class,
            ByteCodec.class,
            BytesCodec.class,
            ClobCodec.class,
            DoubleCodec.class,
            FloatCodec.class,
            GeometryCodec.class,
            IntegerCodec.class,
            JsonCodec.class,
            LocalDateCodec.class,
            LocalDateTimeCodec.class,
            LocalTimeCodec.class,
            LongCodec.class,
            ShortCodec.class,
            StringCodec.class,
            UuidCodec.class,
            ZonedDateTimeCodec.class,
            InstantCodec.class,
            ArrayCodec.class
        );
    }

    @Test
    void decode() {
        assertThat(new DefaultCodecs(mock(Client.class)).decode(ValueInteger.get(100), Value.INTEGER, Integer.class))
            .isEqualTo(100);
    }

    @Test
    void decodeDefaultType() {
        assertThat(new DefaultCodecs(mock(Client.class)).decode(ValueInteger.get(100), Value.INTEGER, Object.class))
            .isEqualTo(100);
    }

    @Test
    void decodeNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).decode(ValueInteger.get(100), Value.INTEGER, null))
            .withMessage("type must not be null");
    }

    @Test
    void decodeNull() {
        assertThat(new DefaultCodecs(mock(Client.class)).decode(null, Value.INTEGER, Integer.class))
            .isNull();
    }

    @Test
    void decodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).decode(ValueInteger.get(100), Value.INTEGER, Void.class))
            .withMessage("Cannot decode value of type java.lang.Void");
    }

    @Test
    void encode() {
        Value parameter = new DefaultCodecs(mock(Client.class)).encode(100);

        assertThat(parameter).isEqualTo(ValueInteger.get(100));
    }

    @Test
    void encodeNoValue() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encode(null))
            .withMessage("value must not be null");
    }

    @Test
    void encodeNull() {
        Value parameter = new DefaultCodecs(mock(Client.class)).encodeNull(Integer.class);

        assertThat(parameter).isEqualTo(ValueNull.INSTANCE);
    }

    @Test
    void encodeNullNoType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encodeNull(null))
            .withMessage("type must not be null");
    }

    @Test
    void encodeNullUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encodeNull(Object.class))
            .withMessage("Cannot encode null parameter of type java.lang.Object");
    }

    @Test
    void encodeUnsupportedType() {
        assertThatIllegalArgumentException().isThrownBy(() -> new DefaultCodecs(mock(Client.class)).encode(new Object()))
            .withMessage("Cannot encode parameter of type java.lang.Object");
    }

    @Test
    void isPresent() {
        assertThat(DefaultCodecs.isPresent(this.getClass().getClassLoader(), "java.lang.Boolean")).isTrue();
    }

    @Test
    void isPresentNotFound() {
        assertThat(DefaultCodecs.isPresent(this.getClass().getClassLoader(), "java.lang.Boolean123456789")).isFalse();
    }
}
