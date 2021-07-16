package com.cloudera.examples.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Utils {
    static public Properties getPrefixedProperties(Properties props, String prefix, boolean removePrefix, List<String> propertiesToIgnore) {
        if (propertiesToIgnore == null)
            propertiesToIgnore = Collections.emptyList();
        Properties prefixedProps = new Properties();
        for (String propName : props.stringPropertyNames()) {
            if (propName.startsWith(prefix) && !propertiesToIgnore.contains(propName)) {
                Object value = props.getProperty(propName);
                if (removePrefix)
                    propName = propName.substring(prefix.length());
                prefixedProps.put(propName, value);
            }
        }
        return prefixedProps;
    }
}
