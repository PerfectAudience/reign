package io.reign.presence;

import org.junit.extensions.cpsuite.ClasspathSuite;
import org.junit.extensions.cpsuite.ClasspathSuite.ClassnameFilters;
import org.junit.runner.RunWith;

@RunWith(value = ClasspathSuite.class)
@ClassnameFilters({ "io.reign.presence.*" })
public class PresenceServiceTestSuite {
}
