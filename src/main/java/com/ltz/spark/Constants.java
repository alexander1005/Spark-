package com.ltz.spark;

import com.ltz.spark.config.ConfigurationManager;

public class Constants {

    //MATE
    public final static String JDBC_URL = ConfigurationManager.get("jdbcurl");
    public final static String JDBC_DRIVER = ConfigurationManager.get("jdbcdriver");

    // USER
    public final static String USER = ConfigurationManager.get("user");
    public final static String PASSWORD = ConfigurationManager.get("password");

    public final static Boolean LOCAL = Boolean.parseBoolean(ConfigurationManager.get("local"));

    //TASK
    public final static String PARAM_END_DATE = "endDate";
    public final static String PARAM_START_DATE = "startDate";
    public final static String PARAM_START_AGE = "startAge";
    public final static String PARAM_END_AGE="endAge";
    public final static String PARAM_PROFESIONALS ="professionals";
    public final static String PARAM_CITYS ="cities";
    public final static String PARAM_SEX = "sex";
    public final static String PARAM_KEYWORDS = "keywords";
    public final static String PARAM_CATEGORY_IDS="categoryIds";
    public final static String FIELD_VISIT_LENGTH = "visitLength";
    public final static String FIELD_STEP_LENGTH = "stepLength";
    public final static String FIELD_START_TIME = "starttime";


    //spark work

    public final static String FIELDS_SESSION_ID = "sessionId";
    public final static String FIELDS_SEARCH_KEYWORDS = "searchKeyWord";
    public final static String FIELDS_CLICK_CATEGORYID = "clickCategoryId";

    public final static String FIELDS_USER_SEX = "userSex";
    public final static String FIELDS_USER_PROFESSION = "userProfessonal";
    public final static String FIELDS_USER_AGE = "userAge";
    public final static String FIELDS_USER_CITY = "userCity";
    public final static String FIELDS_CATEGORY_ID = "categoryId";
    public final static String FIELDS_CLICK_COUNT = "clickCount";
    public final static String FIELDS_ORDER_COUNT = "orderCount";
    public final static String FIELDS_PAY_COUNT = "payCount";



    public final static String SESSION_COUNT = "session_count";
    public final static String TIME_PERIOD_1s_3s = "1s_3s";
    public final static String TIME_PERIOD_4s_6s = "4s_6s";
    public final static String TIME_PERIOD_7s_9s = "7s_9s";
    public final static String TIME_PERIOD_10s_30s = "10s_30s";
    public final static String TIME_PERIOD_30s_60s = "30s_60s";
    public final static String TIME_PERIOD_1m_3m = "1m_3m";
    public final static String TIME_PERIOD_3m_10m = "3m_10m";
    public final static String TIME_PERIOD_10m_30m = "10m_30m";
    public final static String TIME_PERIOD_30m = "30m";
    public final static String STEP_PERIOD_1_3 = "1_3";
    public final static String STEP_PERIOD_4_6 = "4_6";
    public final static String STEP_PERIOD_7_9 = "7_9";
    public final static String STEP_PERIOD_10_30 = "10_30";
    public final static String STEP_PERIOD_30_60 = "30_60";
    public final static String STEP_PERIOD_60 = "60";

    public final static String PARAM_PROFESSIONALS="professionals";
    public final static String PARAM_CITIES="cities";
    public final static String PARAM_TARGET_PAGE_FLOW="targetPageFlow";


}
