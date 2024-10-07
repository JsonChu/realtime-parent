package com.moonpac.realtime.common.bean.vcenter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VcenterParam {
    private String url;
    private String username;
    private String password;
    private String region;
    private Integer intervalSecond;
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VcenterParam that = (VcenterParam) o;
        return Objects.equals(url, that.url) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password) &&
                Objects.equals(intervalSecond, that.intervalSecond);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, username, password, intervalSecond);
    }


}
