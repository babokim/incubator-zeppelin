angular.module('zeppelinWebApp').directive('linkParams', linkParams)

function linkParams () {
  return {
    restrict: 'A',
    replace: true,
    transclude: true,
    template: '<a href="" ng-transclude ng-click="link()"></a>',
    scope: {
      paragraphId: '@',
      params: '@'
    },
    link: function (scope, element, attrs) {
    },
    controller: ['$rootScope', '$scope', function ($rootScope, $scope) {
      $scope.link = function() {
        $scope.paragraphId.split(',').forEach(function(paragraphId) {
          $rootScope.$broadcast('runParagraphForLinkParameter', {
            paragraphId: paragraphId.trim(),
            params: JSON.parse($scope.params)
          });
        });
      }
    }]
  }
}

