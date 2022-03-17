package com.huawei.cloudviews.spark.logicPlanInfo;

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class HiveTableRelationInfo extends BaseInfo{
    public String tableName;

    @Override
    public void getImportInfo(LogicalPlan node, String signature) {
        HiveTableRelation hiveTableRelation = (HiveTableRelation) node;
        this.tableName = ((HiveTableRelation) node).tableMeta().identifier().table();
        this.signature = signature;
        this.logicPlanName = "hivetablerelation";
        this.numChildren = node.children().size();
    }
}
