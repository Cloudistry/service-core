package de.unistuttgart.isw.master;

import static de.unistuttgart.isw.master.Constants.ENV_PROPERTY_PREFIX;

public class PropertyGetter {
	public static String getProperty(String propertyName) {
		return System.getenv(ENV_PROPERTY_PREFIX + propertyName);
	}
}
