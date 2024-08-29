/*
 * Copyright 2017-2018 the original author or authors.
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

package io.github.evaggelosg99.io.r2dbc.h2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.FileSystemUtils;

import io.github.evaggelosg99.io.r2dbc.h2.H2ConnectionConfiguration;
import io.github.evaggelosg99.io.r2dbc.h2.H2ConnectionFactory;
import io.github.evaggelosg99.io.r2dbc.h2.H2ConnectionOption;
import io.github.evaggelosg99.io.r2dbc.h2.client.Client;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

final class H2ConnectionFactoryTest {

    Path basePath;

    @BeforeEach
    void setUp() throws IOException {
        basePath = Files.createTempDirectory("h2-test-");
    }

    @Test
    void constructorNoClientFactory() {
        assertThatIllegalArgumentException().isThrownBy(() -> new H2ConnectionFactory((Mono<? extends Client>) null))
            .withMessage("clientFactory must not be null");
    }

    @Test
    void create() {
        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .url("mem:foo")
            .username("sa")
            .password("")
            .build();

        new H2ConnectionFactory(configuration).create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void createFileBasedDatabase() throws IOException {
        FileSystemUtils.deleteRecursively(basePath);

        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .file(basePath.toString())
            .username("sa")
            .password("")
            .build();

        new H2ConnectionFactory(configuration).create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();

        FileSystemUtils.deleteRecursively(basePath);
    }

    @Test
    void createInMemoryDatabase() {
        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .inMemory("in-memory-named-database")
            .username("sa")
            .password("")
            .build();

        new H2ConnectionFactory(configuration).create()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .verifyComplete();
    }

    @Test
    void getMetadata() {
        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .url("mem")
            .build();

        assertThat(new H2ConnectionFactory(configuration).getMetadata()).isNotNull();
    }

    @Test
    void options() {
        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .inMemory("in-memory-db")
            .option("DB_CLOSE_DELAY=10")
            .build();

        assertThat(configuration.getUrl()).isEqualTo("mem:in-memory-db;DB_CLOSE_DELAY=10");
    }

    @Test
    void individualOptions() {
        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .inMemory("in-memory-db")
            .property("DB_CLOSE_DELAY", "10")
            .build();

        assertThat(configuration.getProperties()).containsEntry("DB_CLOSE_DELAY", "10");
    }

    @Test
    void individualOptionsAsEnum() {
        H2ConnectionConfiguration configuration = H2ConnectionConfiguration.builder()
            .inMemory("in-memory-db")
            .property(H2ConnectionOption.DB_CLOSE_DELAY, "10")
            .build();

        assertThat(configuration.getProperties()).containsEntry("DB_CLOSE_DELAY", "10");
    }

    @Test
    void invalidOptions() {
        assertThatIllegalArgumentException().isThrownBy(() -> {
            H2ConnectionConfiguration.builder()
                .inMemory("in-memory-db")
                .property((String) null, "bar")
                .build();
        }).withMessageContaining("option must not be null");

        assertThatIllegalArgumentException().isThrownBy(() -> {
            H2ConnectionConfiguration.builder()
                .inMemory("in-memory-db")
                .property("some property", null)
                .build();
        }).withMessageContaining("value must not be null");

        assertThatIllegalArgumentException().isThrownBy(() -> {
            H2ConnectionConfiguration.builder()
                .inMemory("in-memory-db")
                .property((H2ConnectionOption) null, "bar")
                .build();
        }).withMessageContaining("option must not be null");

        assertThatIllegalArgumentException().isThrownBy(() -> {
            H2ConnectionConfiguration.builder()
                .inMemory("in-memory-db")
                .property(H2ConnectionOption.DB_CLOSE_DELAY, null)
                .build();
        }).withMessageContaining("value must not be null");
    }
}
