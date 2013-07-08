/*
 Copyright 2013 Yen Pai ypai@reign.io

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package io.reign;

/**
 * 
 * @author ypai
 * 
 */
public interface PathScheme {

    public String getBasePath();

    public String getAbsolutePath(PathType pathType);

    public String getAbsolutePath(PathType pathType, String relativePath);

    public String getAbsolutePath(PathType pathType, String... pathTokens);

    public String joinPaths(String... paths);

    public String[] tokenizePath(String path);

    public String joinTokens(String... pathTokens);

    public boolean isValidToken(String pathToken);

    public boolean isValidPath(String path);

    public boolean isFrameworkClusterId(String clusterId);

    public String getFrameworkClusterId();

    public String getParentPath(String path);

    /**
     * Try to parse input String as canonical ID with some embedded information
     * 
     * @param canonicalId
     * @return Map of values; or null
     * @throws IllegalArgumentException
     *             if there is a parsing error
     */
    public CanonicalId parseCanonicalId(String canonicalId);

    /**
     * 
     * @param canonicalId
     * @return valid path representation of CanonicalId
     */
    public String toPathToken(CanonicalId canonicalId);
}
