package com.huawei.cloudviews.spark.logicPlanInfo;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class LocalLimitInfo extends BaseInfo{
    @Override
    public void getImportInfo(LogicalPlan node, String signature) {
        this.signature = signature;
        this.logicPlanName = "LocalLimit";
        this.numChildren = node.children().size();
    }
}
