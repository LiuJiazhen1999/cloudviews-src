package com.huawei.cloudviews.core.planir.parsers.entities;

public class Query {
  public Plan ParsedPlan;
  
  public Plan AnalyzedPlan;
  
  public Plan OptimizedPlan;
  
  public Plan PhysicalPlan;

  public String operatorName;

  private long queryId;
  
  public void setQueryId(long queryId) {
    this.queryId = queryId;
  }
  
  public long getQueryId() {
    return this.queryId;
  }
}
