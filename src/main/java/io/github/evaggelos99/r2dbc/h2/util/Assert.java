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

package io.github.evaggelos99.r2dbc.h2.util;

import java.util.Objects;

import reactor.util.annotation.Nullable;

/**
 * Utilities for working with {@link Objects}s.
 */
public final class Assert {

	private Assert() {
	}

	/**
	 * Checks that a specified object reference is not {@code null} and throws a
	 * customized {@link IllegalArgumentException} if it is.
	 *
	 * @param t       the object reference to check for nullity
	 * @param message the detail message to be used in the event that an
	 *                {@link IllegalArgumentException} is thrown
	 * @param <T>     the type of the reference
	 * @return {@code t} if not {@code null}
	 * @throws IllegalArgumentException if {@code t} is {code null}
	 */
	public static <T> T requireNonNull(@Nullable final T t, final String message) {
		if (t == null) {
			throw new IllegalArgumentException(message);
		}

		return t;
	}

	/**
	 * Makes sure that b is true
	 *
	 * @param b
	 * @param message
	 * @return true if b is true otherwise throws {@link IllegalArgumentException}
	 */
	public static boolean requireTrue(final boolean b, final String message) {
		if (!b) {
			throw new IllegalArgumentException(message);
		}

		return b;
	}

	/**
	 * Checks that the specified value is of a specific type.
	 *
	 * @param value   the value to check
	 * @param type    the type to require
	 * @param message the message to use in exception if type is not as required
	 * @param <T>     the type being required
	 * @return the value casted to the required type
	 * @throws IllegalArgumentException if {@code value} is not of the required type
	 * @throws NullPointerException     if {@code value}, {@code type}, or
	 *                                  {@code message} is {@code null}
	 */
	@SuppressWarnings("unchecked")
	public static <T> T requireType(final Object value, final Class<T> type, final String message) {
		requireNonNull(value, "value must not be null");
		requireNonNull(type, "type must not be null");
		requireNonNull(message, "message must not be null");

		if (!type.isInstance(value)) {
			throw new IllegalArgumentException(message);
		}

		return (T) value;
	}

}
