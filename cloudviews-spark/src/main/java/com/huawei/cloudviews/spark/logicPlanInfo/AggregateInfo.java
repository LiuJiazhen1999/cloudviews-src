package com.huawei.cloudviews.spark.logicPlanInfo;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.json.simple.*;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AggregateInfo extends BaseInfo{
    public List<String> gruopingList;
    public List<String> aggregateList;

    public AggregateInfo() {
        this.gruopingList = new ArrayList<String>();
        this.aggregateList = new ArrayList<String>();
    }

    @Override
    public void getImportInfo(LogicalPlan node, String signature) {
        Aggregate aggregateNode = ((Aggregate) node);
        List<Expression> groupingExpressions = (List<Expression>) JavaConverters.seqAsJavaListConverter(aggregateNode.groupingExpressions()).asJava();
        for(Expression expression : groupingExpressions) {
            this.gruopingList.add(expression.sql());
        }
        List<NamedExpression> aggregateExpressions = (List<NamedExpression>)JavaConverters.seqAsJavaListConverter(aggregateNode.aggregateExpressions()).asJava();
        for(NamedExpression namedExpression : aggregateExpressions) {
            this.aggregateList.add(((Expression)namedExpression).sql());
        }
        this.signature = signature;
        this.logicPlanName = "aggregate";
        this.numChildren = node.children().size();
    }
}
