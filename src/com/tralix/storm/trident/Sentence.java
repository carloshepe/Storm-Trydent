package com.tralix.storm.trident;

public class Sentence {
    private String sentence;

    public Sentence(final String sentence) {
        this.sentence = sentence;
    }

    public String getSentence() {
        return sentence;
    }

    public void setSentence(final String sentence) {
        this.sentence = sentence;
    }
}
