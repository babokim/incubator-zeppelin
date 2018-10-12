/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

