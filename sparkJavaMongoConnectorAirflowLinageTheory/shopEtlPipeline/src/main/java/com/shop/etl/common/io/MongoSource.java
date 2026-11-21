package com.shop.etl.common.io;

import org.apache.spark.sql.types.StructType;

/**
 * Ek MongoDB source (collection) ni puri olakh: kai collection, kevi rite vanchvi, kevo schema.
 *
 * Raw dataset classes (RawCustomer, RawOrder, RawPayment) darek aa ek object aape che.
 * MongoSourceReader aa object thi j vanche che, etle darek job ne reading code farithi lakhvo
 * nathi padto. Nava collection mate fakt navo MongoSource banavo.
 *
 * @param name           logical naam ("customers"). yml etl.mongo.collections ma actual naam male.
 *                       Aa j naam audit, watermark ane meta_rejected ma "source" tarike lakhay che.
 * @param watermarkField incremental mate field (updated_at). Mongo ma BSON Date hovu joie.
 * @param projection     kaya fields lavva ane kevi rite ($project stage)
 * @param schema         Spark schema. Aapo etle connector sample kari ne schema shodhto nathi
 *                       (fast), ane type pan fix rahe che.
 */
public record MongoSource(String name, String watermarkField, MongoProjection projection, StructType schema) {
}
