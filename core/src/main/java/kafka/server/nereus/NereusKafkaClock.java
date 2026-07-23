/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.nereus;

import org.apache.kafka.common.utils.Time;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/** Borrowed Kafka Time exposed as a wall-clock-only Java Clock. */
public final class NereusKafkaClock extends Clock {
    private final Time time;
    private final ZoneId zone;

    public NereusKafkaClock(Time time) {
        this(time, ZoneOffset.UTC);
    }

    private NereusKafkaClock(Time time, ZoneId zone) {
        this.time = Objects.requireNonNull(time, "time");
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId requestedZone) {
        ZoneId exact = Objects.requireNonNull(requestedZone, "zone");
        return zone.equals(exact) ? this : new NereusKafkaClock(time, exact);
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(time.milliseconds());
    }

    @Override
    public long millis() {
        return time.milliseconds();
    }
}
