package com.sxhs.realtime.bean;

public class TransportItem {
    private Long id;
    private String tubeCode;
    private String packCode;

    public TransportItem(Long id, String tubeCode, String packCode) {
        this.id = id;
        this.tubeCode = tubeCode;
        this.packCode = packCode;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTubeCode() {
        return tubeCode;
    }

    public void setTubeCode(String tubeCode) {
        this.tubeCode = tubeCode;
    }

    public String getPackCode() {
        return packCode;
    }

    public void setPackCode(String packCode) {
        this.packCode = packCode;
    }
}
