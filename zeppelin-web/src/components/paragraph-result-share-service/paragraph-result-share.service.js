angular.module('zeppelinWebApp').service('paragraphResultShareService', paragraphResultShareService)

function paragraphResultShareService () {
  'ngInject'

  let store = {}

  this.clear = function () {
    store = {}
  }

  this.put = function (key, value) {
    store[key] = value
  }

  this.get = function (key) {
    return store[key]
  }

  this.del = function (key) {
    let v = store[key]
    delete store[key]
    return v
  }
}
