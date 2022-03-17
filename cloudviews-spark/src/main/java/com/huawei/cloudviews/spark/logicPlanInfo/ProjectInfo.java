package com.huawei.cloudviews.spark.logicPlanInfo;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ProjectInfo extends BaseInfo{
    public List<String> projectList;

    public ProjectInfo() {
        projectList = new ArrayList<String>();
    }

    @Override
    public void getImportInfo(LogicalPlan node, String signature) {
        Project projectNode = ((Project) node);
        List<NamedExpression> projectList = (List<NamedExpression>) JavaConverters.seqAsJavaListConverter(projectNode.projectList()).asJava();
        for(NamedExpression namedExpression : projectList) {
            this.projectList.add(((Expression)namedExpression).sql());
        }
        this.signature = signature;
        this.logicPlanName = "project";
        this.numChildren = node.children().size();
    }
}
