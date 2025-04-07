package org.sngroup.util;

import java.util.Objects;

public class DevicePort {

    public String deviceName;
    public String portName;

    public DevicePort(String deviceName, String portName) {
        this.deviceName = deviceName;
        this.portName = portName;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public String getPortName() {
        return portName;
    }

    public String getFullName() {
        return getDeviceName() + "_" + getPortName();
    }

    @Override
    public String toString() {
        return getFullName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DevicePort that = (DevicePort) o;
        return Objects.equals(deviceName, that.deviceName) &&
                Objects.equals(portName, that.portName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceName, portName);
    }
}
