package io.tarantool.driver.core.proxy.interfaces;

import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;

/**
 * interface is a contract for adding options when constructing objects of proxy option classes.
 *
 * @author <a href="https://github.com/nickkkccc">Belonogov Nikolay</a>
 */
public interface BuilderOptions {

    void addArgument(ProxyOperationArgument optionName, Object option);
}
