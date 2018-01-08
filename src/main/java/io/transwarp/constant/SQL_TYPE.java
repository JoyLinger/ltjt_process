package io.transwarp.constant;

public enum SQL_TYPE {
    CREATE_DATABASE,
    CREATE_TABLE;

    public static SQL_TYPE parseSqlType(String sqlType) {
        return valueOf(sqlType.toUpperCase());
    }
}
