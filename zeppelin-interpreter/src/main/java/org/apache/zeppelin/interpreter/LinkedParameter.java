package org.apache.zeppelin.interpreter;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yooyoung-mo on 2017. 5. 26..
 */
public class LinkedParameter implements Serializable {
  private String sourceParagraphLinkColumn;
  private String targetParagraphId;
  private List<Object> parameters;

  public LinkedParameter(String sourceParagraphLinkColumn, String targetParagraphId, List<Object> parameters) {
    this.sourceParagraphLinkColumn = sourceParagraphLinkColumn;
    this.targetParagraphId = targetParagraphId;
    this.parameters = parameters;
  }

  public String getSourceParagraphLinkColumn() {
    return sourceParagraphLinkColumn;
  }

  public void setSourceParagraphLinkColumn(String sourceParagraphLinkColumn) {
    this.sourceParagraphLinkColumn = sourceParagraphLinkColumn;
  }

  public String getTargetParagraphId() {
    return targetParagraphId;
  }

  public void setTargetParagraphId(String targetParagraphId) {
    this.targetParagraphId = targetParagraphId;
  }

  public List<Object> getParameters() {
    return parameters;
  }

  public void setParameters(List<Object> parameters) {
    this.parameters = parameters;
  }
}
