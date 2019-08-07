/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.bakdata.recommender.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class ListeningEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 1010532580012980242L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ListeningEvent\",\"namespace\":\"com.bakdata.recommender.avro\",\"fields\":[{\"name\":\"userId\",\"type\":\"long\"},{\"name\":\"artistId\",\"type\":\"long\"},{\"name\":\"albumId\",\"type\":\"long\"},{\"name\":\"trackId\",\"type\":\"long\"},{\"name\":\"timestamp\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();
static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
  }

  private static final BinaryMessageEncoder<ListeningEvent> ENCODER =
      new BinaryMessageEncoder<ListeningEvent>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<ListeningEvent> DECODER =
      new BinaryMessageDecoder<ListeningEvent>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<ListeningEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<ListeningEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<ListeningEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<ListeningEvent>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this ListeningEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a ListeningEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a ListeningEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static ListeningEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public long userId;
  @Deprecated public long artistId;
  @Deprecated public long albumId;
  @Deprecated public long trackId;
  @Deprecated public java.time.Instant timestamp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ListeningEvent() {}

  /**
   * All-args constructor.
   * @param userId The new value for userId
   * @param artistId The new value for artistId
   * @param albumId The new value for albumId
   * @param trackId The new value for trackId
   * @param timestamp The new value for timestamp
   */
  public ListeningEvent(java.lang.Long userId, java.lang.Long artistId, java.lang.Long albumId, java.lang.Long trackId, java.time.Instant timestamp) {
    this.userId = userId;
    this.artistId = artistId;
    this.albumId = albumId;
    this.trackId = trackId;
    this.timestamp = timestamp;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return userId;
    case 1: return artistId;
    case 2: return albumId;
    case 3: return trackId;
    case 4: return timestamp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: userId = (java.lang.Long)value$; break;
    case 1: artistId = (java.lang.Long)value$; break;
    case 2: albumId = (java.lang.Long)value$; break;
    case 3: trackId = (java.lang.Long)value$; break;
    case 4: timestamp = (java.time.Instant)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'userId' field.
   * @return The value of the 'userId' field.
   */
  public long getUserId() {
    return userId;
  }


  /**
   * Sets the value of the 'userId' field.
   * @param value the value to set.
   */
  public void setUserId(long value) {
    this.userId = value;
  }

  /**
   * Gets the value of the 'artistId' field.
   * @return The value of the 'artistId' field.
   */
  public long getArtistId() {
    return artistId;
  }


  /**
   * Sets the value of the 'artistId' field.
   * @param value the value to set.
   */
  public void setArtistId(long value) {
    this.artistId = value;
  }

  /**
   * Gets the value of the 'albumId' field.
   * @return The value of the 'albumId' field.
   */
  public long getAlbumId() {
    return albumId;
  }


  /**
   * Sets the value of the 'albumId' field.
   * @param value the value to set.
   */
  public void setAlbumId(long value) {
    this.albumId = value;
  }

  /**
   * Gets the value of the 'trackId' field.
   * @return The value of the 'trackId' field.
   */
  public long getTrackId() {
    return trackId;
  }


  /**
   * Sets the value of the 'trackId' field.
   * @param value the value to set.
   */
  public void setTrackId(long value) {
    this.trackId = value;
  }

  /**
   * Gets the value of the 'timestamp' field.
   * @return The value of the 'timestamp' field.
   */
  public java.time.Instant getTimestamp() {
    return timestamp;
  }


  /**
   * Sets the value of the 'timestamp' field.
   * @param value the value to set.
   */
  public void setTimestamp(java.time.Instant value) {
    this.timestamp = value;
  }

  /**
   * Creates a new ListeningEvent RecordBuilder.
   * @return A new ListeningEvent RecordBuilder
   */
  public static com.bakdata.recommender.avro.ListeningEvent.Builder newBuilder() {
    return new com.bakdata.recommender.avro.ListeningEvent.Builder();
  }

  /**
   * Creates a new ListeningEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ListeningEvent RecordBuilder
   */
  public static com.bakdata.recommender.avro.ListeningEvent.Builder newBuilder(com.bakdata.recommender.avro.ListeningEvent.Builder other) {
    if (other == null) {
      return new com.bakdata.recommender.avro.ListeningEvent.Builder();
    } else {
      return new com.bakdata.recommender.avro.ListeningEvent.Builder(other);
    }
  }

  /**
   * Creates a new ListeningEvent RecordBuilder by copying an existing ListeningEvent instance.
   * @param other The existing instance to copy.
   * @return A new ListeningEvent RecordBuilder
   */
  public static com.bakdata.recommender.avro.ListeningEvent.Builder newBuilder(com.bakdata.recommender.avro.ListeningEvent other) {
    if (other == null) {
      return new com.bakdata.recommender.avro.ListeningEvent.Builder();
    } else {
      return new com.bakdata.recommender.avro.ListeningEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for ListeningEvent instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ListeningEvent>
    implements org.apache.avro.data.RecordBuilder<ListeningEvent> {

    private long userId;
    private long artistId;
    private long albumId;
    private long trackId;
    private java.time.Instant timestamp;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.bakdata.recommender.avro.ListeningEvent.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.userId)) {
        this.userId = data().deepCopy(fields()[0].schema(), other.userId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.artistId)) {
        this.artistId = data().deepCopy(fields()[1].schema(), other.artistId);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.albumId)) {
        this.albumId = data().deepCopy(fields()[2].schema(), other.albumId);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.trackId)) {
        this.trackId = data().deepCopy(fields()[3].schema(), other.trackId);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[4].schema(), other.timestamp);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
    }

    /**
     * Creates a Builder by copying an existing ListeningEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(com.bakdata.recommender.avro.ListeningEvent other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.userId)) {
        this.userId = data().deepCopy(fields()[0].schema(), other.userId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.artistId)) {
        this.artistId = data().deepCopy(fields()[1].schema(), other.artistId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.albumId)) {
        this.albumId = data().deepCopy(fields()[2].schema(), other.albumId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.trackId)) {
        this.trackId = data().deepCopy(fields()[3].schema(), other.trackId);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[4].schema(), other.timestamp);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'userId' field.
      * @return The value.
      */
    public long getUserId() {
      return userId;
    }


    /**
      * Sets the value of the 'userId' field.
      * @param value The value of 'userId'.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder setUserId(long value) {
      validate(fields()[0], value);
      this.userId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'userId' field has been set.
      * @return True if the 'userId' field has been set, false otherwise.
      */
    public boolean hasUserId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'userId' field.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder clearUserId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'artistId' field.
      * @return The value.
      */
    public long getArtistId() {
      return artistId;
    }


    /**
      * Sets the value of the 'artistId' field.
      * @param value The value of 'artistId'.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder setArtistId(long value) {
      validate(fields()[1], value);
      this.artistId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'artistId' field has been set.
      * @return True if the 'artistId' field has been set, false otherwise.
      */
    public boolean hasArtistId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'artistId' field.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder clearArtistId() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'albumId' field.
      * @return The value.
      */
    public long getAlbumId() {
      return albumId;
    }


    /**
      * Sets the value of the 'albumId' field.
      * @param value The value of 'albumId'.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder setAlbumId(long value) {
      validate(fields()[2], value);
      this.albumId = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'albumId' field has been set.
      * @return True if the 'albumId' field has been set, false otherwise.
      */
    public boolean hasAlbumId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'albumId' field.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder clearAlbumId() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'trackId' field.
      * @return The value.
      */
    public long getTrackId() {
      return trackId;
    }


    /**
      * Sets the value of the 'trackId' field.
      * @param value The value of 'trackId'.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder setTrackId(long value) {
      validate(fields()[3], value);
      this.trackId = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'trackId' field has been set.
      * @return True if the 'trackId' field has been set, false otherwise.
      */
    public boolean hasTrackId() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'trackId' field.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder clearTrackId() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'timestamp' field.
      * @return The value.
      */
    public java.time.Instant getTimestamp() {
      return timestamp;
    }


    /**
      * Sets the value of the 'timestamp' field.
      * @param value The value of 'timestamp'.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder setTimestamp(java.time.Instant value) {
      validate(fields()[4], value);
      this.timestamp = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'timestamp' field has been set.
      * @return True if the 'timestamp' field has been set, false otherwise.
      */
    public boolean hasTimestamp() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'timestamp' field.
      * @return This builder.
      */
    public com.bakdata.recommender.avro.ListeningEvent.Builder clearTimestamp() {
      timestamp = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListeningEvent build() {
      try {
        ListeningEvent record = new ListeningEvent();
        record.userId = fieldSetFlags()[0] ? this.userId : (java.lang.Long) defaultValue(fields()[0]);
        record.artistId = fieldSetFlags()[1] ? this.artistId : (java.lang.Long) defaultValue(fields()[1]);
        record.albumId = fieldSetFlags()[2] ? this.albumId : (java.lang.Long) defaultValue(fields()[2]);
        record.trackId = fieldSetFlags()[3] ? this.trackId : (java.lang.Long) defaultValue(fields()[3]);
        record.timestamp = fieldSetFlags()[4] ? this.timestamp : (java.time.Instant) defaultValue(fields()[4]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<ListeningEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<ListeningEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<ListeningEvent>
    READER$ = (org.apache.avro.io.DatumReader<ListeningEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










