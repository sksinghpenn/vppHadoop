package com.lifotech.vpp.hadoop.stackoverflow.approach2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class StackOverFlowWritable implements Writable {

    private String postID;
    private String postType;
    private String parentID;
    private String dateTimeCreated;
    private String numberOfComments;
    private String comment;

    public StackOverFlowWritable() {
    }

    public StackOverFlowWritable(String postID, String text) {

        this.postID = postID;

        String[] strings = text.split(",");

        int totalElem = strings.length;


        postType = strings[0];
        parentID = strings[1];
        dateTimeCreated = strings[2];
        numberOfComments = strings[3];

        StringBuilder sb = new StringBuilder();

        for (int i = 4; i < totalElem; i++) {
            sb.append(strings[i]);
            if (i < totalElem - 1) {
                sb.append(",");
            }
        }

        this.comment = sb.toString();

    }

    public String getPostID() {
        return postID;
    }

    public void setPostID(String postID) {
        this.postID = postID;
    }

    public String getPostType() {
        return postType;
    }

    public void setPostType(String postType) {
        this.postType = postType;
    }

    public String getParentID() {
        return parentID;
    }

    public void setParentID(String parentID) {
        this.parentID = parentID;
    }

    public String getDateTimeCreated() {
        return dateTimeCreated;
    }

    public void setDateTimeCreated(String dateTimeCreated) {
        this.dateTimeCreated = dateTimeCreated;
    }

    public String getNumberOfComments() {
        return numberOfComments;
    }

    public void setNumberOfComments(String numberOfComments) {
        this.numberOfComments = numberOfComments;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {


        dataOutput.writeUTF(postID);
        dataOutput.writeUTF(postType);
        dataOutput.writeUTF(parentID);
        dataOutput.writeUTF(dateTimeCreated);
        dataOutput.writeUTF(numberOfComments);
        dataOutput.writeUTF(comment);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        postID = dataInput.readUTF();
        postType = dataInput.readUTF();
        parentID = dataInput.readUTF();
        dateTimeCreated = dataInput.readUTF();
        numberOfComments = dataInput.readUTF();
        comment = dataInput.readUTF();

    }
}
