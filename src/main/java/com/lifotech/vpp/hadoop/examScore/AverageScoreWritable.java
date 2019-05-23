package com.lifotech.vpp.hadoop.examScore;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageScoreWritable implements Writable {

    private String subject;
    private int score;


    public AverageScoreWritable(String subject, int score) {
        this.subject = subject;
        this.score = score;
    }

    public AverageScoreWritable() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(subject);
        dataOutput.writeInt(score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        subject = dataInput.readUTF();
        score = dataInput.readInt();
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
