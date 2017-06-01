package org.apache.zeppelin.interpreter;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yooyoung-mo on 2017. 5. 26..
 */
public class LinkedParameter implements Serializable {
  private String sourceParagraphId;
  private String targetParagraph;
  private int sourceParagraphLinkColumnIdx;
  private List<Object> targetParagraphLinkParams;

  public LinkedParameter(String sourceParagraphId, int sourceParagraphLinkColumnIdx,
                         String targetParagraph, List<Object> targetParagraphLinkParams) {
    this.sourceParagraphId = sourceParagraphId;
    this.targetParagraph = targetParagraph;
    this.sourceParagraphLinkColumnIdx = sourceParagraphLinkColumnIdx;
    this.targetParagraphLinkParams = targetParagraphLinkParams;
  }

  public String getSourceParagraphId() {
    return sourceParagraphId;
  }

  public void setSourceParagraphId(String sourceParagraphId) {
    this.sourceParagraphId = sourceParagraphId;
  }

  public String getTargetParagraph() {
    return targetParagraph;
  }

  public void setTargetParagraph(String targetParagraph) {
    this.targetParagraph = targetParagraph;
  }

  public List<Object> getTargetParagraphLinkParams() {
    return targetParagraphLinkParams;
  }

  public void setTargetParagraphLinkParams(List<Object> targetParagraphLinkParams) {
    this.targetParagraphLinkParams = targetParagraphLinkParams;
  }

  public int getSourceParagraphLinkColumnIdx() {
    return sourceParagraphLinkColumnIdx;
  }

  public void setSourceParagraphLinkColumnIdx(int sourceParagraphLinkColumnIdx) {
    this.sourceParagraphLinkColumnIdx = sourceParagraphLinkColumnIdx;
  }
}
