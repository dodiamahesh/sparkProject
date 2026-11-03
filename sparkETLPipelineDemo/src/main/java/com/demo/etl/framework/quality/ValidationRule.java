package com.demo.etl.framework.quality;

import org.apache.spark.sql.Column;

/**
 * One data-quality rule.
 *
 * @param errorType what kind of problem this rule detects
 * @param failWhen  Spark condition that is TRUE when the record is BAD
 * @param message   text stored in the rejected table (a Column, so it can include the bad value)
 */
public record ValidationRule(ErrorType errorType, Column failWhen, Column message) {
}
