package com.huawei.cloudviews.spark.signature;

import com.alibaba.fastjson.JSONObject;
import com.huawei.cloudviews.core.signatures.Signature;
import com.huawei.cloudviews.core.signatures.hash.SignHash64;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.huawei.cloudviews.spark.logicPlanInfo.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import scala.Array;
import scala.Function0;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.runtime.AbstractFunction0;

public abstract class LogicalPlanSignature implements Signature<LogicalPlan> {
  public static final int MAX_TO_STRING_FIELDS = 10000;
  
  private static LogicalPlanSignature _hts;
  
  private static LogicalPlanSignature _ht;
  
  public String getSignature(LogicalPlan node) {//返回值信息是一个json串，包括该算子需要的所有信息
    String signature = null;
    List<LogicalPlan> children = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(node.children()).asJava();
    List<String> childSignatures = new ArrayList<String>();
    for (LogicalPlan child : children) {
      childSignatures.add(getSignature(child));
    }
    signature = SignHash64.Compute(signature, getLocalSignature(node));
    Collections.sort(childSignatures);//考虑与顺序无关的情况
    for (String childSignature : childSignatures) {
      signature = SignHash64.Compute(signature, childSignature);
    }//当前signature为以当前节点为子树的总哈希值
    return signature;
  }


  public Boolean isComputeLocalSignature(LogicalPlan node) {
    if(HiveTableRelation.class.isAssignableFrom(node.getClass())) {
      return true;
    } else if(Union.class.isAssignableFrom(node.getClass())) {
      return true;
    }
    return false;
  }

  public static String getLocalInfo(String signature, LogicalPlan node) {
    if(Aggregate.class.isAssignableFrom(node.getClass())) {//处理Aggregate算子的信息
      AggregateInfo aggregateInfo = new AggregateInfo();
      aggregateInfo.getImportInfo(node, signature);
      return JSONObject.toJSONString(aggregateInfo);
    } else if(Union.class.isAssignableFrom(node.getClass())) {
      UnionInfo unionInfo = new UnionInfo();
      unionInfo.getImportInfo(node, signature);
      return JSONObject.toJSONString(unionInfo);
    } else if(Project.class.isAssignableFrom(node.getClass())) {//处理Project算子的信息
      ProjectInfo projectInfo = new ProjectInfo();
      projectInfo.getImportInfo(node, signature);
      return JSONObject.toJSONString(projectInfo);
    } else if(HiveTableRelation.class.isAssignableFrom(node.getClass())) {//处理HivetableRelation算子的信息
      HiveTableRelationInfo hiveTableRelationInfo = new HiveTableRelationInfo();
      hiveTableRelationInfo.getImportInfo(node, signature);
      return JSONObject.toJSONString(hiveTableRelationInfo);
    } else if(GlobalLimit.class.isAssignableFrom(node.getClass())) {
      GlobalLimitInfo globalLimitInfo = new GlobalLimitInfo();
      globalLimitInfo.getImportInfo(node, signature);
      return JSONObject.toJSONString(globalLimitInfo);
    } else if(LocalLimit.class.isAssignableFrom(node.getClass())) {
      LocalLimitInfo localLimitInfo = new LocalLimitInfo();
      localLimitInfo.getImportInfo(node, signature);
      return JSONObject.toJSONString(localLimitInfo);
    }
    return "{}";
  }

  public String getLocalSignature(LogicalPlan node) {//计算节点内部信息的哈希值并且将重要信息写入日志
    if(isComputeLocalSignature(node)) {//需要计算当前节点信息的哈希
      if(Union.class.isAssignableFrom(node.getClass())) {
        return SignHash64.Compute("Union");
      } else if(HiveTableRelation.class.isAssignableFrom(node.getClass())) {
        return SignHash64.Compute("HiveTableRelation", ((HiveTableRelation) node).tableMeta().identifier().table());
      }
    }
    return null;//不需要计算当前节点的哈希值，直接返回null
  }
  
  private String getArgSignature(Object arg) {
    if (Literal.class.isAssignableFrom(arg.getClass()))//arg是否是Literal的子类
      return getLiteralSignature((Literal)arg); 
    if (CatalogTable.class.isAssignableFrom(arg.getClass()))
      return getCatalogTableSignature((CatalogTable)arg); 
    if (AttributeReference.class.isAssignableFrom(arg.getClass())) {
      AttributeReference attrRef = (AttributeReference)arg;
      return SignHash64.Compute(attrRef.name());
    } 
    if (Expression.class.isAssignableFrom(arg.getClass())) {
      Expression canExpr = (Expression)arg;
      String treeSig = SignHash64.Compute(canExpr.nodeName());
      List<Expression> children = (List<Expression>)JavaConverters.seqAsJavaListConverter(canExpr.children()).asJava();
      if (children == null || children.size() == 0)
        return treeSig; 
      for (Expression child : children)
        treeSig = SignHash64.Compute(treeSig, getArgSignature(child)); 
      return treeSig;
    } 
    if (Array.class.isAssignableFrom(arg.getClass())) {
      Object[] javaArray = (Object[])arg;
      String arraySig = null;
      for (Object element : javaArray)
        arraySig = SignHash64.Compute(arraySig, getArgSignature(element)); 
      return arraySig;
    } 
    if (Seq.class.isAssignableFrom(arg.getClass())) {
      List<Object> javaList = (List<Object>)JavaConverters.seqAsJavaListConverter((Seq)arg).asJava();
      String listSig = null;
      for (Object element : javaList)
        listSig = SignHash64.Compute(listSig, getArgSignature(element)); 
      return listSig;
    } 
    if (Set.class.isAssignableFrom(arg.getClass())) {
      java.util.Set<Object> javaSet = (java.util.Set<Object>)JavaConverters.setAsJavaSetConverter((Set)arg).asJava();
      String setSig = null;
      for (Object element : javaSet)
        setSig = SignHash64.Compute(setSig, getArgSignature(element)); 
      return setSig;
    } 
    if (TreeNode.class.isAssignableFrom(arg.getClass())) {
      TreeNode treeNode = (TreeNode)arg;
      String treeSig = SignHash64.Compute(treeNode.nodeName());
      List<LogicalPlan> children = (List<LogicalPlan>)JavaConverters.seqAsJavaListConverter(treeNode.children()).asJava();
      if (children == null || children.size() == 0)
        return treeSig; 
      for (LogicalPlan child : children)
        treeSig = SignHash64.Compute(treeSig, getArgSignature(child)); 
      return treeSig;
    } 
    return null;
  }
  
  public static String getSignatures(LogicalPlan node) {//计算哈希值

    StringBuffer sb = new StringBuffer();
    sb.append("HTS:");
    sb.append(" ");
    sb.append(",HT:");
    sb.append(HT(node));//只关心HT函数，上面的HTS不看
    return sb.toString();
  }
  
  public static String HTS(LogicalPlan node) {
    if (_hts == null)
      _hts = new LogicalPlanSignature() {
          public String getLiteralSignature(Literal literal) {
            return SignHash64.Compute(literal.simpleString());
          }
          
          public String getCatalogTableSignature(CatalogTable catalogTable) {
            StringBuilder versionedTableIdentifer = new StringBuilder();
            versionedTableIdentifer.append(catalogTable.identifier().toString());
            versionedTableIdentifer.append(catalogTable.createTime());
            Map<String, String> properties = catalogTable.properties();
            String ddlTime = LogicalPlanSignature.getValueFromMap("transient_lastDdlTime", properties);
            versionedTableIdentifer.append(ddlTime);
            Map<String, String> ignoredProperties = catalogTable.ignoredProperties();
            String numFiles = LogicalPlanSignature.getValueFromMap("numFiles", ignoredProperties);
            versionedTableIdentifer.append(numFiles);
            String totalSize = LogicalPlanSignature.getValueFromMap("totalSize", ignoredProperties);
            versionedTableIdentifer.append(totalSize);
            return SignHash64.Compute(versionedTableIdentifer.toString());
          }
        }; 
    return _hts.getSignature(node);
  }
  
  private static String getValueFromMap(String key, Map<String, String> props) {
    String value = (String)props.getOrElse(key, (Function0)new AbstractFunction0<String>() {
          public String apply() {
            return "";
          }
        });
    return value;
  }
  
  public static String HT(LogicalPlan node) {
    if (_ht == null)
      _ht = new LogicalPlanSignature() {
          public String getLiteralSignature(Literal literal) {
            return "";
          }
          
          public String getCatalogTableSignature(CatalogTable catalogTable) {
            return SignHash64.Compute(catalogTable.identifier().toString());
          }
        }; 
    return _ht.getSignature(node);//这个函数先只关心这个
  }
  
  public abstract String getLiteralSignature(Literal paramLiteral);
  
  public abstract String getCatalogTableSignature(CatalogTable paramCatalogTable);
}
