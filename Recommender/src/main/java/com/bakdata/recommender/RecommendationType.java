package com.bakdata.recommender;

public enum RecommendationType {
    ALBUM,
    ARTIST,
    TRACK;

    public String getLeftIndexName() {
        return String.format("%s_%s", this, RecommenderMain.LEFT_INDEX_NAME);
    }


    public String getRightIndexName() {
        return String.format("%s_%s", this, RecommenderMain.RIGHT_INDEX_NAME);
    }
}
