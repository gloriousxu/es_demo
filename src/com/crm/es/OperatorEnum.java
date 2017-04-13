package com.crm.es;

/**
 * @author xuchao
 * @create 2017/4/13.
 */
//操作符枚举
public enum OperatorEnum {
    GTE("gte"),GT("gt"),LT("lt"),LTE("lte"),EQ("eq"),BETWEEN("between"),DEFAULT("");
    String value;
    OperatorEnum(String value){
        this.value = value;
    }
}
