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

package io.github.evaggelos99.io.r2dbc.h2;

import io.github.evaggelos99.io.r2dbc.h2.codecs.Codecs;
import io.github.evaggelos99.io.r2dbc.h2.util.Assert;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An implementation of {@link Result} representing the results of a query against an H2 database.
 */
public final class H2Result implements Result {

    private final H2RowMetadata rowMetadata;

    private final Flux<H2Row> rows;

    private final Mono<Long> rowsUpdated;

    private final Flux<? extends Segment> segments;

    H2Result(H2RowMetadata rowMetadata, Flux<H2Row> rows, Mono<Long> rowsUpdated, Flux<? extends Segment> segments) {
        this.rowMetadata = rowMetadata;
        this.rows = Assert.requireNonNull(rows, "rows must not be null");
        this.rowsUpdated = Assert.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
        this.segments = Assert.requireNonNull(segments, "segments must not be null");
    }

    private H2Result(Mono<Long> rowsUpdated, Flux<Segment> segments) {
        this.rowMetadata = null;
        this.rows = Flux.empty();
        this.rowsUpdated = Assert.requireNonNull(rowsUpdated, "rowsUpdated must not be null");
        this.segments = Assert.requireNonNull(segments, "segments must not be null");
    }

    @Override
    public Mono<Long> getRowsUpdated() {
        return this.rowsUpdated;
    }

    @Override
    public H2Result filter(Predicate<Segment> filter) {
        Assert.requireNonNull(filter, "predicate must not be null");

        Flux<? extends Segment> filteredSegments = this.segments.filter(filter::test);

        return new H2Result(this.rowMetadata, this.rows, this.rowsUpdated, filteredSegments);
    }

    @Override
    public <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> f) {
        Assert.requireNonNull(f, "f must not be null");

        return this.segments
            .flatMap(segment -> {
                Publisher<? extends T> result = f.apply(segment);

                if (result == null) {
                    return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
                }

                if (result instanceof Mono) {
                    return result;
                }

                return Flux.from(result);
            });
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Assert.requireNonNull(f, "f must not be null");

        return this.rows
            .map(row -> f.apply(row, this.rowMetadata));
    }

    @Override
    public String toString() {
        return "H2Result{" +
            ", rowMetadata=" + this.rowMetadata +
            ", rows=" + this.rows +
            ", rowsUpdated=" + this.rowsUpdated +
            '}';
    }

    static H2Result toResult(Codecs codecs, @Nullable Long rowsUpdated) {
        Assert.requireNonNull(codecs, "codecs must not be null");

        return new H2Result(Mono.justOrEmpty(rowsUpdated), Flux.just((UpdateCount) () -> rowsUpdated));
    }

    static H2Result toResult(Codecs codecs, ResultInterface result, @Nullable Long rowsUpdated) {
        Assert.requireNonNull(codecs, "codecs must not be null");
        Assert.requireNonNull(result, "result must not be null");

        H2RowMetadata rowMetadata = H2RowMetadata.toRowMetadata(codecs, result);

        Iterable<Value[]> iterable = () -> new Iterator<Value[]>() {

            @Override
            public boolean hasNext() {
                boolean b = result.hasNext();

                if (!b) {
                    result.close();
                }

                return b;
            }

            @Override
            public Value[] next() {
                result.next();
                return result.currentRow();
            }
        };

        Flux<H2Row> rows = Flux.fromIterable(iterable)
            .map(values -> H2Row.toRow(values, result, codecs, rowMetadata))
            .onErrorMap(DbException.class, H2DatabaseExceptionFactory::convert);

        return new H2Result(rowMetadata, rows, Mono.justOrEmpty(rowsUpdated), rows);
    }
}
