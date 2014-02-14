package io.reign.coord;

import org.junit.extensions.cpsuite.ClasspathSuite;
import org.junit.extensions.cpsuite.ClasspathSuite.ClassnameFilters;
import org.junit.runner.RunWith;

@RunWith(value = ClasspathSuite.class)
@ClassnameFilters({ "io.reign.coord.*" })
public class CoordServiceTestSuite {

}
