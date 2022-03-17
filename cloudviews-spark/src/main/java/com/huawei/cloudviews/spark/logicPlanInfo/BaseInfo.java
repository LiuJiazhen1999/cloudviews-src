package com.huawei.cloudviews.spark.logicPlanInfo;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class BaseInfo {
    public String signature;//哈希值
    public String logicPlanName;//算子名称
    public Integer numChildren;
    public void getImportInfo(LogicalPlan node, String signature) {
        this.signature = signature;
    }
}
