package com.crm.es;

/**
 * @author xuchao
 * @create 2017/4/12.
 */

//字段属性枚举
public enum FieldEnum {
    BUILDER("builder"),TYPE("types");

    private String value;
    FieldEnum(String value) {
        this.value = value;
    }

    @Override
    public String toString() {

        return String.valueOf(this.value);

    }

}
