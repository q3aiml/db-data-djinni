package net.q3aiml.dbdata.jdbc;

import net.q3aiml.dbdata.util.MoreCollectors;
import net.q3aiml.dbdata.config.SqlConnectionInfo;

import javax.sql.DataSource;
import java.beans.*;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;

public class DataSources {
    public static DataSource toDataSource(SqlConnectionInfo info) {
        Class<? extends DataSource> dataSourceClass = dataSourceClass(info);
        DataSource dataSource;
        try {
            dataSource = dataSourceClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("error creating datasource of type " + dataSourceClass
                    + ": " + e.getMessage(), e);
        }

        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(dataSourceClass);
        } catch (IntrospectionException e) {
            throw new UnsupportedOperationException("unable to get bean info of datasource class " + dataSourceClass
                    + ": " + e.getMessage(), e);
        }

        Map<String, PropertyDescriptor> propertyDescriptors = asList(beanInfo.getPropertyDescriptors()).stream()
                .filter(prop -> prop.getWriteMethod() != null
                        && Arrays.equals(prop.getWriteMethod().getParameterTypes(), new Class[]{ String.class }))
                .collect(MoreCollectors.toMap(d -> d.getName().toLowerCase(), identity()));

        for (Map.Entry<String, String> prop : info.props.entrySet()) {
            PropertyDescriptor propertyDescriptor = propertyDescriptors.get(prop.getKey());
            if (propertyDescriptor == null) {
                throw new IllegalArgumentException("datasource " + dataSourceClass.getName() + " does not have "
                        + "writable property " + prop.getKey() + "; available properties: "
                        + propertyDescriptors.keySet());
            }

            try {
                propertyDescriptor.getWriteMethod().invoke(dataSource, prop.getValue());
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new UnsupportedOperationException("unable to set property " + prop.getKey() + " on datasource "
                        + prop.getKey());
            }
        }

        return dataSource;
    }

    private static Class<? extends DataSource> dataSourceClass(SqlConnectionInfo info) {
        checkNotNull(info.type, "must contain 'type' with full class name of jdbc DataSource implementation: {}", info);

        try {
            return Class.forName(info.type).asSubclass(DataSource.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("invalid 'type', class not found: " + e.getMessage(), e);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("invalid 'type', not a DataSource subclass: " + e.getMessage(), e);
        }
    }
}
