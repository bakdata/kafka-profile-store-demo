package com.bakdata.profilestore.core;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import java.time.Instant;
import java.util.Objects;

public class ListeningEventBuilder {
    private long userId;
    private long artistId;
    private long albumId;
    private long trackId;
    private Instant instant;

    public ListeningEventBuilder setUserId(final long userId) {
        this.userId = userId;
        return this;
    }

    public ListeningEventBuilder setArtistId(final long artistId) {
        this.artistId = artistId;
        return this;
    }

    public ListeningEventBuilder setAlbumId(final long albumId) {
        this.albumId = albumId;
        return this;
    }

    public ListeningEventBuilder setTrackId(final long trackId) {
        this.trackId = trackId;
        return this;
    }


    public ListeningEventBuilder setTimestamp(final Instant instant) {
        this.instant = instant;
        return this;
    }

    public ListeningEvent build() {
        Objects.requireNonNull(this.instant);
        return new ListeningEvent(this.userId, this.artistId, this.albumId, this.trackId, this.instant);
    }

}
