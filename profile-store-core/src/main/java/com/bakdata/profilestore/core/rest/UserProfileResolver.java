package com.bakdata.profilestore.core.rest;

import com.bakdata.profilestore.core.avro.NamedChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import javax.ws.rs.ext.ContextResolver;

public class UserProfileResolver implements ContextResolver<ObjectMapper> {
    private final ObjectMapper objectMapper;

    public UserProfileResolver() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.objectMapper.addMixIn(UserProfile.class, IgnoreAvroProperties.class);
        this.objectMapper.addMixIn(NamedChartRecord.class, IgnoreAvroProperties.class);

    }

    @Override
    public ObjectMapper getContext(final Class<?> aClass) {
        return this.objectMapper;
    }

    abstract static class IgnoreAvroProperties {
        @JsonIgnore
        abstract void getSchema();

        @JsonIgnore
        abstract void getSpecificData();

        @JsonIgnore
        abstract void getConversion();
    }
}
