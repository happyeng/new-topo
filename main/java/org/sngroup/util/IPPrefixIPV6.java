package org.sngroup.util;

import java.util.Objects;

public class IPPrefixIPV6 {
    public String ip;
    public int prefix;

    public IPPrefixIPV6(String ip, int prefix){
        this.ip = ip;
        this.prefix = prefix;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, prefix);
    }
}
