package com.huawei.cloudviews.spark.logicPlanInfo;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class UnionInfo extends BaseInfo{
    @Override
    public void getImportInfo(LogicalPlan node, String signature) {
        this.logicPlanName = "union";
        this.signature = signature;
        this.numChildren = node.children().size();
    }
}
