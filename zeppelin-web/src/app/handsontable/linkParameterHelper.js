/**
 * LinkParameterHelper class
 */
export default class LinkParameterHelper {
  constructor (columnNames, resultRows) {
    this.columnNames = columnNames || []
    this.resultRows = resultRows || []
  }

  _parseHTML(html) {
    var parser = new DOMParser();
    return parser.parseFromString(html, 'text/html')
  }

  _getDataRow(html) {
    var htmlDoc = this._parseHTML(html)
    return htmlDoc.getElementsByTagName('a')[0].getAttribute('data-raw')
  }

  // _getRawResultData(idx, resultRow) {
  //   if(this.isLinkParameterHtmlTag(resultRow[idx])) {
  //     return this._getDataRow(resultRow[idx])
  //   } else {
  //     return resultRow[idx];
  //   }
  // }

  _findIdxByColumnName(columnName, columnNames) {
    return columnNames.indexOf(columnName)
  }

  _makeParams(targetParagraphLinkParameters, resultRow) {
    var params = {}

    for(var i = 0; i < targetParagraphLinkParameters.length; i++) {
      var linkParameter = targetParagraphLinkParameters[i];

      // Key로 데이터 Get
      var data = resultRow[this._findIdxByColumnName(Object.keys(linkParameter)[0], this.columnNames)]

      if(this.isLinkParameterHtmlTag(data)) {
        data = this._getDataRow(data)
      }

      // Value로 key값으로 매핑 후 데이터 Set
      params[Object.values(linkParameter)[0]] = data;
    }
    return JSON.stringify(params);
  }

  generateLinkParameters(linkedParameters) {
    this._clearLinkParameterHtmlTagToResultRow();

    // Column별 Link 처리
    for(var i = 0; i < linkedParameters.length; i++) {
      var linkedParameter = linkedParameters[i];

      var idx = this._findIdxByColumnName(linkedParameter.sourceParagraphLinkColumn, this.columnNames);

      if(idx >= 0) {
        for(var j = 0; j < this.resultRows.length; j++) {
          var resultRow = this.resultRows[j];

          // var rawResultData = this._getRawResultData(idx, resultRow);
          var params = this._makeParams(linkedParameter.parameters, resultRow);
          resultRow[idx] =
            '<a link-params data-raw="' + resultRow[idx] + '" data-paragraph-id="' + linkedParameter.targetParagraphId
            + '" data-params=' + '\'' + params + '\'' + '>' + resultRow[idx] + '</a>';

          this.resultRows[j] = resultRow;
        }
      }
    }

    return this.resultRows;
  }

  _clearLinkParameterHtmlTagToResultRow() {
    for(var i = 0; i < this.resultRows.length; i++) {
      var resultRow = this.resultRows[i];

      for(var j = 0; j < resultRow.length; j++) {
        if(this.isLinkParameterHtmlTag(resultRow[j])) {
          resultRow[j] = this._getDataRow(resultRow[j])
        }
      }
    }
  }

  isLinkParameterHtmlTag(text) {
    if(text.indexOf('<a link-params') !== -1) {
      return true;
    } else {
      return false;
    }
  }
}
