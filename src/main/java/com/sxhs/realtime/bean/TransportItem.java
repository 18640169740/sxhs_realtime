package com.sxhs.realtime.bean;

import lombok.Data;

@Data
public class TransportItem {
    private Long id;
    private String tubeCode;
    private String packCode;

    public TransportItem() {
    }

    public TransportItem(Long id, String tubeCode, String packCode) {
        this.id = id;
        this.tubeCode = tubeCode;
        this.packCode = packCode;
    }
}
