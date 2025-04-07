package org.sngroup.verifier;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Vector;

public class CibMessage {
    public Vector<Announcement> announcements;
    public List<Integer> withdraw;

    public CibMessage() {
        announcements = new Vector<>();
        withdraw = new LinkedList<>();
    }

    public CibMessage(List<Announcement> announcement, List<Integer> withdraw, int sourceIndex){
        this.announcements = new Vector<>(announcement);
        this.withdraw=withdraw;
    }
}
