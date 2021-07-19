/*
 * Copyright (c) 2021, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.examples.utils;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Utils {
    static public Properties getPrefixedProperties(Properties props, String prefix, boolean removePrefix, List<String> propertiesToIgnore) {
        if (propertiesToIgnore == null)
            propertiesToIgnore = Collections.emptyList();
        Properties selectedProps = new Properties();
        for (String propName : props.stringPropertyNames()) {
            if (propName.startsWith(prefix)) {
                Object value = props.getProperty(propName);
                if (removePrefix)
                    propName = propName.substring(prefix.length());
                if (!propertiesToIgnore.contains(propName)) {
                    selectedProps.put(propName, value);
                }
            }
        }
        return selectedProps;
    }
}
